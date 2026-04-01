#!/usr/bin/env python3
"""
原始 naiveRL PPO 实现 - 用于对比测试
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

# ==================== 配置 ====================
config = {
    'env_name': 'CartPole-v0',
    'horizon': 128,
    'train_loop': 10,
    'max_episode': 200,
    'gamma': 0.99,
    'lambda': 0.95,
    'learning_rate': 3e-4,
    'epsilon_clip': 0.2,
    'entropy_coeff': 0.01,
    'vf_loss_coeff': 0.5,
    'max_grad_norm': 40,  # 原始代码用的是40
    'device': 'cuda' if torch.cuda.is_available() else 'cpu'
}


# ==================== 模型 (原始版) ====================
class Model(nn.Module):
    def __init__(self, obs_dim, act_dim, hidden=128):
        super(Model, self).__init__()
        self.fc1 = nn.Linear(obs_dim, hidden)
        self.fc_pi = nn.Linear(hidden, act_dim)
        self.fc_v = nn.Linear(hidden, 1)

    def pi(self, x):
        x = F.relu(self.fc1(x))
        x = F.softmax(self.fc_pi(x), dim=-1)
        return x

    def v(self, x):
        x = F.relu(self.fc1(x))  # 重复计算!
        v = self.fc_v(x)
        return v

    def pi_v(self, x):
        x = F.relu(self.fc1(x))
        v = self.fc_v(x)
        x = F.softmax(self.fc_pi(x), dim=-1)
        return x, v


# ==================== 原始 PPO 算法 ====================
class Alg:
    def __init__(self, model):
        self.model = model
        self.gamma = config['gamma']
        self.lam = config['lambda']
        self.entropy_coeff = config['entropy_coeff']
        self.vf_loss_coeff = config['vf_loss_coeff']
        self.optimizer = optim.Adam(self.model.parameters(), lr=config['learning_rate'])
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model.to(self.device)
        self.old_pi = copy.deepcopy(model)

    def learn(self, states, actions, rewards, s_, done):
        for i in range(config['train_loop']):
            s_final = torch.unsqueeze(torch.FloatTensor(s_).to(self.device), 0)
            R = 0.0 if done else self.old_pi.v(s_final).item()
            
            # 原始代码：用CPU计算，然后转numpy
            states_t = torch.FloatTensor(states).to(self.device)
            values = self.old_pi.v(states_t).cpu().detach().numpy()
            
            r_lst = np.clip(rewards, -1, 1)
            tds = r_lst + self.gamma * np.append(values[1:], [[R]], axis=0) - values
            
            # 原始：使用 scipy.signal.lfilter
            advantage = signal.lfilter([1.0], [1.0, -self.gamma * self.lam], tds[::-1])[::-1]
            td_target = advantage + values
            
            advantage = torch.tensor(advantage.copy(), dtype=torch.float).to(self.device)
            td_target = torch.tensor(td_target.copy(), dtype=torch.float).to(self.device)
            
            actions_t = torch.LongTensor(actions).to(self.device)
            
            pi_a = self.model.pi(states_t).gather(1, actions_t.unsqueeze(1))
            old_pi_a = self.old_pi.pi(states_t).gather(1, actions_t.unsqueeze(1))
            ratio = torch.exp(torch.log(pi_a + 1e-8) - torch.log(old_pi_a + 1e-8))
            surr1 = ratio * advantage.detach()
            surr2 = torch.clamp(ratio, 1 - config['epsilon_clip'], 1 + config['epsilon_clip']) * advantage.detach()
            policy_loss = -torch.min(surr1, surr2).mean()
            
            value_clip = self.old_pi.v(states_t) + (self.model.v(states_t) - self.old_pi.v(states_t)).clamp(
                -config['epsilon_clip'], config['epsilon_clip'])
            v_loss1 = (self.model.v(states_t) - td_target.detach()).pow(2)
            v_loss2 = (value_clip - td_target.detach()).pow(2)
            value_loss = torch.max(v_loss1, v_loss2).mean()
            
            entropy_loss = (-torch.log(self.model.pi(states_t) + 1e-8) * torch.exp(
                torch.log(self.model.pi(states_t) + 1e-8))).mean()
            
            loss = policy_loss + self.vf_loss_coeff * value_loss + self.entropy_coeff * entropy_loss

            self.optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(self.model.parameters(), config['max_grad_norm'])
            self.optimizer.step()
        
        self.old_pi.load_state_dict(self.model.state_dict())

    def get_action(self, state):
        state_t = torch.FloatTensor(state).unsqueeze(0).to(self.device)
        with torch.no_grad():
            pi = self.model.pi(state_t)
            dist = torch.distributions.Categorical(pi)
            action = dist.sample().item()
            value = self.model.v(state_t).item()
            log_prob = dist.log_prob(torch.LongTensor([action]).to(self.device)).item()
        return action, log_prob, value
    
    def get_value(self, state):
        state_t = torch.FloatTensor(state).unsqueeze(0).to(self.device)
        with torch.no_grad():
            return self.model.v(state_t).item()


class Agent:
    def __init__(self, alg):
        self.alg = alg
    
    def sample(self, state):
        return self.alg.get_action(state)[0]
    
    def learn(self, states, actions, rewards, s_, done):
        self.alg.learn(states, actions, rewards, s_, done)


def run_original_ppo(env_name='CartPole-v0', max_episode=200):
    """运行原始 PPO"""
    print(f"\n{'='*50}")
    print(f"原始 PPO - 环境: {env_name}")
    print(f"设备: {config['device']}")
    print(f"{'='*50}\n")
    
    env = gym.make('CartPole-v1')
    obs_dim = env.observation_space.shape[0]
    act_dim = env.action_space.n
    
    model = Model(obs_dim, act_dim)
    alg = Alg(model)
    agent = Agent(alg)
    
    episode_rewards = []
    start = datetime.datetime.now()
    
    for episode in range(max_episode):
        score = 0.0
        s = env.reset()
        if isinstance(s, tuple):
            s = s[0]
        
        done = False
        states, actions, rewards = [], [], []
        
        for t in range(config['horizon']):
            a = agent.sample(s)
            step_result = env.step(a)
            if len(step_result) == 4:
                s_, r, done, info = step_result
            else:
                s_, r, terminated, truncated, info = step_result
                done = terminated or truncated
            
            if isinstance(s_, tuple):
                s_ = s_[0]
            
            states.append(s)
            actions.append(a)
            rewards.append(r)
            s = s_
            score += r
            
            if done:
                break
        
        agent.learn(states, actions, rewards, s, done)
        episode_rewards.append(score)
        
        if episode % 20 == 0:
            avg = np.mean(episode_rewards[-20:]) if len(episode_rewards) >= 20 else np.mean(episode_rewards)
            print(f"Episode {episode}: avg = {avg:.2f}")
    
    end = datetime.datetime.now()
    elapsed = (end - start).seconds
    avg_last20 = np.mean(episode_rewards[-20:]) if len(episode_rewards) >= 20 else np.mean(episode_rewards)
    max_reward = max(episode_rewards)
    
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
        'rewards': episode_rewards
    }


if __name__ == '__main__':
    results = run_original_ppo()