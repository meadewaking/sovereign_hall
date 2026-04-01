#!/usr/bin/env python3
"""
PPO-Refine: 优化版 PPO 实现
对比原始 naiveRL 的 PPO，优化点：
1. 共享 fc1 计算 (pi_v 方法)
2. 纯 PyTorch 实现 GAE (避免 scipy CPU-GPU 切换)
3. 向量化动作采样
4. 使用 torch.compile (PyTorch 2.0+)
"""
import datetime
import gym
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F

# ==================== 配置 ====================
config = {
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
    'max_grad_norm': 0.5,
    'device': 'cuda' if torch.cuda.is_available() else 'cpu'
}


# ==================== 模型 (优化版) ====================
class Model(nn.Module):
    def __init__(self, obs_dim, act_dim, hidden=128):
        super().__init__()
        self.fc1 = nn.Linear(obs_dim, hidden)
        self.fc_pi = nn.Linear(hidden, act_dim)
        self.fc_v = nn.Linear(hidden, 1)
        
    def forward(self, x):
        x = F.relu(self.fc1(x))
        return x
    
    def pi(self, x):
        x = self.forward(x)
        return F.softmax(self.fc_pi(x), dim=-1)
    
    def v(self, x):
        x = self.forward(x)
        return self.fc_v(x)
    
    # 优化：共享前向计算
    def pi_v(self, x):
        x = self.forward(x)
        return F.softmax(self.fc_pi(x), dim=-1), self.fc_v(x)


# ==================== GAE 计算 (纯 PyTorch) ====================
def compute_gae(values, rewards, next_value, done, gamma, lam, device):
    """纯 PyTorch 实现 GAE"""
    advantages = torch.zeros_like(rewards).to(device)
    last_adv = 0
    
    for t in reversed(range(len(rewards))):
        next_val = next_value if t == len(rewards) - 1 else values[t + 1]
        delta = rewards[t] + gamma * next_val * (1 - done[t]) - values[t]
        advantages[t] = last_adv = delta + gamma * lam * (1 - done[t]) * last_adv
    
    return advantages, advantages + values


# ==================== PPO 优化算法 ====================
class PPO:
    def __init__(self, obs_dim, act_dim, device='cuda'):
        self.device = device
        self.epsilon = config['epsilon_clip']
        self.entropy_coeff = config['entropy_coeff']
        self.vf_coeff = config['vf_loss_coeff']
        self.max_grad_norm = config['max_grad_norm']
        
        # 创建两个独立的模型
        self.model = Model(obs_dim, act_dim).to(device)
        self.old_model = Model(obs_dim, act_dim).to(device)
        self.old_model.load_state_dict(self.model.state_dict())
        
        self.optimizer = optim.Adam(self.model.parameters(), lr=config['learning_rate'])
        
        # torch.compile 优化
        self.use_compile = False
        if hasattr(torch, 'compile'):
            try:
                self.model = torch.compile(self.model, backend='eager')
                self.use_compile = True
                print(f"✓ torch.compile 启用")
            except Exception as e:
                print(f"✗ torch.compile 不可用: {e}")
    
    @torch.no_grad()
    def get_action(self, state):
        state = np.array(state, dtype=np.float32)
        state = torch.FloatTensor(state).unsqueeze(0).to(self.device)
        pi = self.model.pi(state)
        dist = torch.distributions.Categorical(pi)
        action = dist.sample()
        return action.item(), dist.log_prob(action).item(), self.model.v(state).item()
    
    @torch.no_grad()
    def get_value(self, state):
        state = np.array(state, dtype=np.float32)
        return self.model.v(torch.FloatTensor(state).unsqueeze(0).to(self.device)).item()
    
    def update(self, states, actions, old_log_probs, advantages, returns):
        states = torch.FloatTensor(np.array(states, dtype=np.float32)).to(self.device)
        actions = torch.LongTensor(actions).to(self.device)
        old_log_probs = torch.FloatTensor(old_log_probs).to(self.device)
        advantages = ((advantages - advantages.mean()) / (advantages.std() + 1e-8)).to(self.device)
        returns = returns.to(self.device)
        
        for _ in range(config['train_loop']):
            pi, values = self.model.pi_v(states)
            dist = torch.distributions.Categorical(pi)
            
            ratio = torch.exp(dist.log_prob(actions) - old_log_probs)
            surr1 = ratio * advantages
            surr2 = torch.clamp(ratio, 1 - self.epsilon, 1 + self.epsilon) * advantages
            policy_loss = -torch.min(surr1, surr2).mean()
            
            values_old = self.old_model.v(states).detach()
            values_clipped = values_old + (values - values_old).clamp(-self.epsilon, self.epsilon)
            value_loss = torch.max((values - returns).pow(2), (values_clipped - returns).pow(2)).mean()
            
            loss = policy_loss + self.vf_coeff * value_loss - self.entropy_coeff * dist.entropy().mean()
            
            self.optimizer.zero_grad()
            loss.backward()
            nn.utils.clip_grad_norm_(self.model.parameters(), self.max_grad_norm)
            self.optimizer.step()
        
        # 更新旧模型
        if self.use_compile:
            orig_state = {}
            for k, v in self.model.state_dict().items():
                if k.startswith('_orig_mod.'):
                    orig_state[k[10:]] = v
                else:
                    orig_state[k] = v
            self.old_model.load_state_dict(orig_state)
        else:
            self.old_model.load_state_dict(self.model.state_dict())


def collect_trajectories(env, agent, horizon):
    states, actions, rewards, values, log_probs, dones = [], [], [], [], [], []
    state = env.reset()
    if isinstance(state, tuple):
        state = state[0]
    state = np.array(state, dtype=np.float32)
    
    for _ in range(horizon):
        action, log_prob, value = agent.get_action(state)
        
        step_result = env.step(action)
        if len(step_result) == 4:
            next_state, reward, done, _ = step_result
        else:
            next_state, reward, terminated, truncated, _ = step_result
            done = terminated or truncated
        
        if isinstance(next_state, tuple):
            next_state = next_state[0]
        next_state = np.array(next_state, dtype=np.float32)
        
        states.append(state)
        actions.append(action)
        rewards.append(reward)
        values.append(value)
        log_probs.append(log_prob)
        dones.append(done)
        
        state = next_state if not done else env.reset()
        if isinstance(state, tuple):
            state = state[0]
        state = np.array(state, dtype=np.float32)
    
    last_state = state
    return states, actions, rewards, values, log_probs, dones, agent.get_value(last_state), last_state


def train_ppo(env, agent, max_episode):
    episode_rewards = []
    
    for ep in range(max_episode):
        states, actions, rewards, values, log_probs, dones, final_value, _ = \
            collect_trajectories(env, agent, config['horizon'])
        
        advantages, returns = compute_gae(
            torch.FloatTensor(values + [final_value]).to(agent.device)[:-1],
            torch.FloatTensor(rewards).to(agent.device),
            torch.tensor(final_value).to(agent.device),
            torch.FloatTensor(dones).to(agent.device),
            config['gamma'], config['lambda'], agent.device
        )
        
        agent.update(states, actions, log_probs, advantages, returns)
        
        episode_rewards.append(sum(rewards))
        
        if ep % 20 == 0:
            avg = np.mean(episode_rewards[-20:]) if len(episode_rewards) >= 20 else np.mean(episode_rewards)
            print(f"Episode {ep}: avg = {avg:.2f}")
    
    return episode_rewards


def run_benchmark(env_name='CartPole-v1', max_episode=200):
    """运行基准测试"""
    print(f"\n{'='*50}")
    print(f"环境: {env_name}")
    print(f"设备: {config['device']}")
    print(f"{'='*50}\n")
    
    env = gym.make(env_name)
    agent = PPO(env.observation_space.shape[0], env.action_space.n, config['device'])
    
    start = datetime.datetime.now()
    rewards = train_ppo(env, agent, max_episode)
    end = datetime.datetime.now()
    
    elapsed = (end - start).seconds
    avg_last20 = np.mean(rewards[-20:]) if len(rewards) >= 20 else np.mean(rewards)
    max_reward = max(rewards)
    
    print(f"\n{'='*50}")
    print(f"训练完成!")
    print(f"  耗时: {elapsed}秒")
    print(f"  最后20局平均: {avg_last20:.2f}")
    print(f"  最高奖励: {max_reward:.2f}")
    print(f"{'='*50}")
    
    return {
        'time': elapsed,
        'avg_reward': avg_last20,
        'max_reward': max_reward,
        'rewards': rewards
    }


if __name__ == '__main__':
    results = run_benchmark()