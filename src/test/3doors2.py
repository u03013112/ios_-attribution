import random

def simulate_monty_hall_modified(num_trials, switch):
    wins = 0
    for _ in range(num_trials):
        # 随机选择汽车的位置
        car_position = random.randint(0, 2)
        
        # 参赛者随机选择一扇门
        contestant_choice = random.randint(0, 2)
        
        # 判断主持人是否会打开一扇门
        if contestant_choice == car_position:
            # 主持人打开一扇有山羊的门
            remaining_doors = [i for i in range(3) if i != contestant_choice and i != car_position]
            host_open = random.choice(remaining_doors)
            
            # 如果参赛者选择换门
            if switch:
                contestant_choice = [i for i in range(3) if i != contestant_choice and i != host_open][0]
        
        # 判断参赛者是否赢得汽车
        if contestant_choice == car_position:
            wins += 1
    
    return wins

# 进行实验
num_trials = 10000

# 不换门策略
wins_no_switch = simulate_monty_hall_modified(num_trials, switch=False)
print(f"不换门策略的胜率: {wins_no_switch / num_trials * 100}%")

# 换门策略
wins_switch = simulate_monty_hall_modified(num_trials, switch=True)
print(f"换门策略的胜率: {wins_switch / num_trials * 100}%")
