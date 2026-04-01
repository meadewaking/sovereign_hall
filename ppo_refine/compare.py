#!/usr/bin/env python3
"""
PPO 对比测试：原始版 vs 优化版
"""
import datetime
import gym
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
from scipy import signal
import copy
import json

# ==================== 统一配置 ====================
CONFIG = {
    'env_name': 'CartPole-v1',
    'horizon': 128,
    'train_loop': 10,
    'max_episode': 200,
    'gamma': 0.99,
    'lambda': 0.95,
    'learning_rate': 3e-4,
    'epsilon_clip': 0.2,
    'entropy_coeff': 0.01,
    'vf_loss_coeff': 0.5,
    'device': 'cuda' if torch.cuda.is_available() else 'cpu'
}


# ==================== 模型 (原始版) ====================
class ModelOriginal(nn.Module):
    def __init__(self, obs_dim, act_dim, hidden=128):
        super().__init__()
        self.fc1 = nn.Linear(obs_dim, hidden)
        self.fc_pi = nn.Linear(hidden, act_dim)
        self.fc_v = nn.Linear(hidden, 1)

    def pi(self, x):
        x = F.relu(self.fc1(x))
        return F.softmax(self.fc_pi(x), dim=-1)

    def v(self, x):
        x = F.relu(self.fc1(x))  # 重复计算!
        return self.fc_v(x)

    def pi_v(self, x):
        x = F.relu(self.fc1(x))
        return F.softmax(self.fc_pi(x), dim=-1), self.fc_v(x)


# ==================== 模型 (优化版) ====================
class ModelOptimized(nn.Module):
    def __init__(self, obs_dim, act_dim, hidden=128):
        super().__init__()
        self.fc1 = nn.Linear(obs_dim, hidden)
        self.fc_pi = nn.Linear(hidden, act_dim)
        self.fc_v = nn.Linear(hidden, 1)

    def forward(self, x):
        return F.relu(self.fc1(x))

    def pi(self, x):
        x = self.forward(x)
        return F.softmax(self.fc_pi(x), dim=-1)

    def v(self, x):
        x = self.forward(x)
        return self.fc_v(x)

    def pi_v(self, x):
        x = self.forward(x)
        return F.softmax(self.fc_pi(x), dim=-1), self.fc_v(x)


# ==================== GAE 计算 (优化版) ====================
def compute_gae(values, rewards, next_value, done, gamma, lam, device):
    advantages = torch.zeros_like(rewards).to(device)
    last_adv = 0
    for t in reversed(range(len(rewards))):
        next_val = next_value if t == len(rewards) - 1 else values[t + 1]
        delta = rewards[t] + gamma * next_val * (1 - done[t]) - values[t]
        advantages[t] = last_adv = delta + gamma * lam * (1 - done[t]) * last_adv
    return advantages, advantages + values


# ==================== 原始 PPO ====================
class PPOOriginal:
    def __init__(self, obs_dim, act_dim, device):
        self.device = device
        self.model = ModelOriginal(obs_dim, act_dim).to(device)
        self.old_model = ModelOriginal(obs_dim, act_dim).to(device)
        # 处理 torch.compile 包装的模型
        state = self.model.state_dict()
        if any(k.startswith('_orig_mod.') for k in state.keys()):
            # unwrap torch.compile
            new_state = {k[10:]: v for k, v in state.items() if k.startswith('_orig_mod.')}
            self.old_model.load_state_dict(new_state)
        else:
            self.old_model.load_state_dict(state)
        self.optimizer = optim.Adam(self.model.parameters(), lr=CONFIG['learning_rate'])

    def learn(self, states, actions, rewards, s_, done):
        for _ in range(CONFIG['train_loop']):
            s_final = torch.FloatTensor(s_).unsqueeze(0).to(self.device)
            R = 0.0 if done else self.old_model.v(s_final).item()
            
            states_t = torch.FloatTensor(np.array(states)).to(self.device)
            values = self.old_model.v(states_t).cpu().detach().numpy()
            
            r_lst = np.clip(rewards, -1, 1)
            tds = r_lst + CONFIG['gamma'] * np.append(values[1:], [[R]], axis=0) - values
            
            # 使用 scipy.signal.lfilter (原始方法)
            advantage = signal.lfilter([1.0], [1.0, -CONFIG['gamma'] * CONFIG['lambda']], tds[::-1])[::-1]
            td_target = advantage + values
            
            advantage = torch.tensor(advantage.copy(), dtype=torch.float).to(self.device)
            td_target = torch.tensor(td_target.copy(), dtype=torch.float).to(self.device)
            actions_t = torch.LongTensor(actions).to(self.device)
            
            pi_a = self.model.pi(states_t).gather(1, actions_t.unsqueeze(1))
            old_pi_a = self.old_model.pi(states_t).gather(1, actions_t.unsqueeze(1))
            ratio = torch.exp(torch.log(pi_a + 1e-8) - torch.log(old_pi_a + 1e-8))
            surr1 = ratio * advantage.detach()
            surr2 = torch.clamp(ratio, 1 - CONFIG['epsilon_clip'], 1 + CONFIG['epsilon_clip']) * advantage.detach()
            policy_loss = -torch.min(surr1, surr2).mean()
            
            value_clip = self.old_model.v(states_t) + (self.model.v(states_t) - self.old_model.v(states_t)).clamp(
                -CONFIG['epsilon_clip'], CONFIG['epsilon_clip'])
            v_loss1 = (self.model.v(states_t) - td_target.detach()).pow(2)
            v_loss2 = (value_clip - td_target.detach()).pow(2)
            value_loss = torch.max(v_loss1, v_loss2).mean()
            
            entropy_loss = (-torch.log(self.model.pi(states_t) + 1e-8) * torch.exp(
                torch.log(self.model.pi(states_t) + 1e-8))).mean()
            
            loss = policy_loss + CONFIG['vf_loss_coeff'] * value_loss + CONFIG['entropy_coeff'] * entropy_loss
            
            self.optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(self.model.parameters(), 40)
            self.optimizer.step()
        
        # 处理 torch.compile 包装的模型
        state = self.model.state_dict()
        if any(k.startswith('_orig_mod.') for k in state.keys()):
            # unwrap torch.compile
            new_state = {k[10:]: v for k, v in state.items() if k.startswith('_orig_mod.')}
            self.old_model.load_state_dict(new_state)
        else:
            self.old_model.load_state_dict(state)

    @torch.no_grad()
    def get_action(self, state):
        state = np.array(state, dtype=np.float32)
        state_t = torch.FloatTensor(state).unsqueeze(0).to(self.device)
        pi = self.model.pi(state_t)
        dist = torch.distributions.Categorical(pi)
        action = dist.sample()
        return action.item(), dist.log_prob(action).item(), self.model.v(state_t).item()


# ==================== 优化版 PPO ====================
class PPOOptimized:
    def __init__(self, obs_dim, act_dim, device):
        self.device = device
        self.model = ModelOptimized(obs_dim, act_dim).to(device)
        self.old_model = ModelOptimized(obs_dim, act_dim).to(device)
        # 处理 torch.compile 包装的模型
        state = self.model.state_dict()
        if any(k.startswith('_orig_mod.') for k in state.keys()):
            # unwrap torch.compile
            new_state = {k[10:]: v for k, v in state.items() if k.startswith('_orig_mod.')}
            self.old_model.load_state_dict(new_state)
        else:
            self.old_model.load_state_dict(state)
        self.optimizer = optim.Adam(self.model.parameters(), lr=CONFIG['learning_rate'])
        
        # torch.compile
        if hasattr(torch, 'compile'):
            try:
                self.model = torch.compile(self.model, backend='eager')
                print(f"  ✓ torch.compile 启用")
            except:
                pass

    @torch.no_grad()
    def get_action(self, state):
        state = np.array(state, dtype=np.float32)
        state_t = torch.FloatTensor(state).unsqueeze(0).to(self.device)
        pi = self.model.pi(state_t)
        dist = torch.distributions.Categorical(pi)
        action = dist.sample()
        return action.item(), dist.log_prob(action).item(), self.model.v(state_t).item()

    def update(self, states, actions, old_log_probs, advantages, returns):
        states = torch.FloatTensor(np.array(states, dtype=np.float32)).to(self.device)
        actions = torch.LongTensor(actions).to(self.device)
        old_log_probs = torch.FloatTensor(old_log_probs).to(self.device)
        advantages = ((advantages - advantages.mean()) / (advantages.std() + 1e-8)).to(self.device)
        returns = returns.to(self.device)
        
        for _ in range(CONFIG['train_loop']):
            pi, values = self.model.pi_v(states)
            dist = torch.distributions.Categorical(pi)
            
            ratio = torch.exp(dist.log_prob(actions) - old_log_probs)
            surr1 = ratio * advantages
            surr2 = torch.clamp(ratio, 1 - CONFIG['epsilon_clip'], 1 + CONFIG['epsilon_clip']) * advantages
            policy_loss = -torch.min(surr1, surr2).mean()
            
            values_old = self.old_model.v(states).detach()
            values_clipped = values_old + (values - values_old).clamp(-CONFIG['epsilon_clip'], CONFIG['epsilon_clip'])
            value_loss = torch.max((values - returns).pow(2), (values_clipped - returns).pow(2)).mean()
            
            loss = policy_loss + CONFIG['vf_loss_coeff'] * value_loss + CONFIG['entropy_coeff'] * dist.entropy().mean()
            
            self.optimizer.zero_grad()
            loss.backward()
            # 处理 torch.compile 包装的模型
            params = list(self.model.parameters())
            nn.utils.clip_grad_norm_(params, 40)
            self.optimizer.step()
        
        # 处理 torch.compile 包装的模型
        state = self.model.state_dict()
        if any(k.startswith('_orig_mod.') for k in state.keys()):
            # unwrap torch.compile
            new_state = {k[10:]: v for k, v in state.items() if k.startswith('_orig_mod.')}
            self.old_model.load_state_dict(new_state)
        else:
            self.old_model.load_state_dict(state)


# ==================== 训练函数 ====================
def train_original(env, agent, max_episode):
    rewards = []
    for ep in range(max_episode):
        s = env.reset()
        if isinstance(s, tuple): s = s[0]
        s = np.array(s, dtype=np.float32)
        
        states, actions, rews = [], [], []
        done = False
        
        for _ in range(CONFIG['horizon']):
            a, _, _ = agent.get_action(s)
            result = env.step(a)
            if len(result) == 4:
                s_, r, done, _ = result
            else:
                s_, r, term, trunc, _ = result
                done = term or trunc
            
            if isinstance(s_, tuple): s_ = s_[0]
            s_ = np.array(s_, dtype=np.float32)
            
            states.append(s)
            actions.append(a)
            rews.append(r)
            s = s_
            if done: break
        
        agent.learn(states, actions, rews, s, done)
        rewards.append(sum(rews))
        
        if ep % 20 == 0:
            avg = np.mean(rewards[-20:]) if len(rewards) >= 20 else np.mean(rewards)
            print(f"  Episode {ep}: avg = {avg:.2f}")
    return rewards


def train_optimized(env, agent, max_episode):
    rewards = []
    for ep in range(max_episode):
        states, actions, rews, values, log_probs, dones = [], [], [], [], [], []
        s = env.reset()
        if isinstance(s, tuple): s = s[0]
        s = np.array(s, dtype=np.float32)
        
        for _ in range(CONFIG['horizon']):
            a, lp, v = agent.get_action(s)
            result = env.step(a)
            if len(result) == 4:
                s_, r, done, _ = result
            else:
                s_, r, term, trunc, _ = result
                done = term or trunc
            
            if isinstance(s_, tuple): s_ = s_[0]
            s_ = np.array(s_, dtype=np.float32)
            
            states.append(s)
            actions.append(a)
            rews.append(r)
            values.append(v)
            log_probs.append(lp)
            dones.append(done)
            s = s_
            if done: break
        
        # 计算 GAE
        with torch.no_grad():
            final_val = agent.model.v(torch.FloatTensor(s).unsqueeze(0).to(agent.device)).item()
        
        advantages, returns = compute_gae(
            torch.FloatTensor(values + [final_val]).to(agent.device)[:-1],
            torch.FloatTensor(rews).to(agent.device),
            torch.tensor(final_val).to(agent.device),
            torch.FloatTensor(dones).to(agent.device),
            CONFIG['gamma'], CONFIG['lambda'], agent.device
        )
        
        agent.update(states, actions, log_probs, advantages, returns)
        rewards.append(sum(rews))
        
        if ep % 20 == 0:
            avg = np.mean(rewards[-20:]) if len(rewards) >= 20 else np.mean(rewards)
            print(f"  Episode {ep}: avg = {avg:.2f}")
    return rewards


# ==================== 主测试 ====================
def run_comparison():
    env_name = 'CartPole-v1'
    max_ep = 500
    
    print(f"\n{'='*60}")
    print(f"PPO 性能对比测试")
    print(f"环境: {env_name}, 设备: {CONFIG['device']}, Episode: {max_ep}")
    print(f"{'='*60}")
    
    # 测试原始版
    print(f"\n[1] 原始 PPO (naiveRL)")
    print("-" * 40)
    env1 = gym.make(env_name)
    obs_dim = env1.observation_space.shape[0]
    act_dim = env1.action_space.n
    agent1 = PPOOriginal(obs_dim, act_dim, CONFIG['device'])
    
    start = datetime.datetime.now()
    r1 = train_original(env1, agent1, max_ep)
    t1 = (datetime.datetime.now() - start).seconds
    
    r1_avg = np.mean(r1[-20:])
    r1_max = max(r1)
    print(f"  耗时: {t1}秒, 最后20局平均: {r1_avg:.2f}, 最高: {r1_max:.2f}")
    
    # 测试优化版
    print(f"\n[2] 优化版 PPO (PPO-Refine)")
    print("-" * 40)
    env2 = gym.make(env_name)
    agent2 = PPOOptimized(obs_dim, act_dim, CONFIG['device'])
    
    start = datetime.datetime.now()
    r2 = train_optimized(env2, agent2, max_ep)
    t2 = (datetime.datetime.now() - start).seconds
    
    r2_avg = np.mean(r2[-20:])
    r2_max = max(r2)
    print(f"  耗时: {t2}秒, 最后20局平均: {r2_avg:.2f}, 最高: {r2_max:.2f}")
    
    # 结果对比
    print(f"\n{'='*60}")
    print(f"对比结果")
    print(f"{'='*60}")
    print(f"{'指标':<20} {'原始版':>12} {'优化版':>12} {'提升':>12}")
    print(f"{'-'*60}")
    print(f"{'耗时(秒)':<20} {t1:>12} {t2:>12} {(t1-t2)/t1*100:>11.1f}%")
    print(f"{'最后20局平均':<20} {r1_avg:>12.2f} {r2_avg:>12.2f} {(r2_avg-r1_avg)/r1_avg*100:>11.1f}%")
    print(f"{'最高奖励':<20} {r1_max:>12.2f} {r2_max:>12.2f} {(r2_max-r1_max)/r1_max*100:>11.1f}%")
    print(f"{'='*60}")
    
    return {
        'original': {'time': t1, 'avg': r1_avg, 'max': r1_max},
        'optimized': {'time': t2, 'avg': r2_avg, 'max': r2_max}
    }


if __name__ == '__main__':
    results = run_comparison()