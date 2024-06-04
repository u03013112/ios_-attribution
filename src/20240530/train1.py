import pandas as pd  
from sklearn.model_selection import train_test_split  
from sklearn.ensemble import RandomForestClassifier  
from sklearn.metrics import classification_report, accuracy_score, recall_score, make_scorer  
from sklearn.model_selection import GridSearchCV    

def classify_and_evaluate():
    # 加载数据并筛选特征和目标列
    df = pd.read_csv('/src/data/lwData20240530.csv')
    
    # 只分析24小时内不付费用户
    freeDf = df.loc[(df['payNew24'] == 0)]
    
    # 选取目标列并转换为二分类问题
    freeDf['target'] = freeDf['payNew168'].apply(lambda x: 1 if x > 0 else 0)
    
    # 分离出目标为1的样本以及目标为0的样本
    positive_samples = freeDf[freeDf['target'] == 1]
    negative_samples = freeDf[freeDf['target'] == 0].sample(frac=0.1, random_state=0)
    
    # 合并进行采样后的数据
    sampled_df = pd.concat([positive_samples, negative_samples])

    # 选取特征列
    X = sampled_df[['heroLevelUp', 'appLaunch', 'login', 'payAction', 'plunder', 'goldCost', 'onlineTime', 'mainLevel', 'radar']]
    
    # 重新选取目标列
    y = sampled_df['target']
    
    # 将数据集分为训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
    
    # 初始化随机森林分类器
    n_estimators = 100  # 设定树的数量
    rf_clf = RandomForestClassifier(n_estimators=0, random_state=0, warm_start=True, n_jobs=-1)
    
    # 增量增加树并逐步训练，每训练10次进行一次评估
    for i in range(n_estimators):
        rf_clf.n_estimators = i + 1
        rf_clf.fit(X_train, y_train)
        
        if (i + 1) % 10 == 0:
            print(f'Trained {i + 1}/{n_estimators} trees')
            # 在训练集上预测
            y_train_pred = rf_clf.predict(X_train)
            # 在测试集上预测
            y_test_pred = rf_clf.predict(X_test)

            # 输出训练集表现
            print("Training Performance:")
            print(classification_report(y_train, y_train_pred))
            print('Training Accuracy:', accuracy_score(y_train, y_train_pred))

            # 输出测试集表现
            print("Test Performance:")
            print(classification_report(y_test, y_test_pred))
            print('Test Accuracy:', accuracy_score(y_test, y_test_pred))

# 添加早停
def classify_and_evaluate_et():
    # 加载数据并筛选特征和目标列
    df = pd.read_csv('/src/data/lwData20240530.csv')
    
    # 只分析24小时内不付费用户
    freeDf = df.loc[(df['payNew24'] == 0)]
    
    # 选取目标列并转换为二分类问题
    freeDf['target'] = freeDf['payNew168'].apply(lambda x: 1 if x > 0 else 0)
    
    # 分离出目标为1的样本以及目标为0的样本
    positive_samples = freeDf[freeDf['target'] == 1]
    negative_samples = freeDf[freeDf['target'] == 0].sample(frac=0.1, random_state=0)
    
    # 合并进行采样后的数据
    sampled_df = pd.concat([positive_samples, negative_samples])

    # 选取特征列
    X = sampled_df[['heroLevelUp', 'appLaunch', 'login', 'payAction', 'plunder', 'goldCost', 'onlineTime', 'mainLevel', 'radar']]
    
    # 重新选取目标列
    y = sampled_df['target']
    
    # 将数据集分为训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
    
    # 初始化随机森林分类器
    n_estimators = 100  # 设定树的数量
    rf_clf = RandomForestClassifier(n_estimators=1, random_state=0, warm_start=True, n_jobs=-1)
    
    # 早停参数
    best_val_score = 0
    best_n_estimators = 0
    patience = 10  # 早停的耐心值
    no_improvement_count = 0

    # 增量增加树并逐步训练，每训练10次进行一次评估
    for i in range(1, n_estimators + 1):
        rf_clf.n_estimators = i
        rf_clf.fit(X_train, y_train)
        
        # 在验证集上评估
        y_val_pred = rf_clf.predict(X_test)
        val_score = accuracy_score(y_test, y_val_pred)
        
        if val_score > best_val_score:
            best_val_score = val_score
            best_n_estimators = i
            no_improvement_count = 0
        else:
            no_improvement_count += 1
        
        if no_improvement_count >= patience:
            print(f'Early stopping at {i} trees')
            break
        
        if i % 10 == 0:
            print(f'Trained {i}/{n_estimators} trees')
            # 在训练集上预测
            y_train_pred = rf_clf.predict(X_train)
            # 在测试集上预测
            y_test_pred = rf_clf.predict(X_test)

            # 输出训练集表现
            print("Training Performance:")
            print(classification_report(y_train, y_train_pred))
            print('Training Accuracy:', accuracy_score(y_train, y_train_pred))

            # 输出测试集表现
            print("Test Performance:")
            print(classification_report(y_test, y_test_pred))
            print('Test Accuracy:', accuracy_score(y_test, y_test_pred))
    
    # 最终输出最佳树的数量和对应的表现
    print(f'Best number of trees: {best_n_estimators}, Best Validation Accuracy: {best_val_score:.4f}')
    
    # 使用最佳树的数量重新训练模型
    rf_clf.n_estimators = best_n_estimators
    rf_clf.fit(X_train, y_train)
    
    # 在训练集上预测
    y_train_pred = rf_clf.predict(X_train)
    # 在测试集上预测
    y_test_pred = rf_clf.predict(X_test)

    # 输出最终训练集表现
    print("Final Training Performance:")
    print(classification_report(y_train, y_train_pred))
    print('Training Accuracy:', accuracy_score(y_train, y_train_pred))

    # 输出最终测试集表现
    print("Final Test Performance:")
    print(classification_report(y_test, y_test_pred))
    print('Test Accuracy:', accuracy_score(y_test, y_test_pred))

# 调用函数
classify_and_evaluate_et()


# 调整权重
def classify_and_evaluate2():
    # 加载数据并筛选特征和目标列
    df = pd.read_csv('/src/data/lwData20240530.csv')
    
    # 只分析24小时内不付费用户
    freeDf = df.loc[(df['payNew24'] == 0)]
    # 选取目标列并转换为二分类问题
    freeDf['target'] = freeDf['payNew168'].apply(lambda x: 1 if x > 0 else 0)
    
    # 分离出目标为1的样本以及目标为0的样本
    positive_samples = freeDf[freeDf['target'] == 1]
    negative_samples = freeDf[freeDf['target'] == 0].sample(frac=0.1, random_state=0)
    
    # 合并进行采样后的数据
    sampled_df = pd.concat([positive_samples, negative_samples])
    
    # 选取特征列
    X = sampled_df[['heroLevelUp', 'appLaunch', 'login', 'payAction', 'plunder', 'goldCost', 'onlineTime', 'mainLevel', 'radar']]
    
    # 重新选取目标列
    y = sampled_df['target']

    # 将数据集分为训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
    
    # 初始化随机森林分类器，设置 class_weight='balanced' 或者定制权重
    n_estimators = 100  # 设定树的数量
    rf_clf = RandomForestClassifier(n_estimators=0, random_state=0, warm_start=True, n_jobs=-1, class_weight={0: 1, 1: 10})  # 使付费用户权重更高
    
    # 增量增加树并逐步训练，每训练10次进行一次评估
    for i in range(n_estimators):
        rf_clf.n_estimators = i + 1
        rf_clf.fit(X_train, y_train)
        
        if (i + 1) % 10 == 0:
            print(f'Trained {i + 1}/{n_estimators} trees')

            # 在训练集上预测
            y_train_pred = rf_clf.predict(X_train)
            
            # 在测试集上预测
            y_test_pred = rf_clf.predict(X_test)
            
            # 输出训练集表现
            print("Training Performance:")
            print(classification_report(y_train, y_train_pred))
            print('Training Accuracy:', accuracy_score(y_train, y_train_pred))

            # 输出测试集表现
            print("Test Performance:")
            print(classification_report(y_test, y_test_pred))
            print('Test Accuracy:', accuracy_score(y_test, y_test_pred))
  
def classify_and_evaluate3():    
    # 加载数据并筛选特征和目标列    
    df = pd.read_csv('/src/data/lwData20240530.csv')    
        
    # 只分析24小时内不付费用户  
    freeDf = df.loc[df['payNew24'] == 0]  
        
    # 选取目标列并转换为二分类问题  
    freeDf['target'] = freeDf['payNew168'].apply(lambda x: 1 if x > 0 else 0)  
        
    # 分离出目标为1的样本以及目标为0的样本  
    positive_samples = freeDf[freeDf['target'] == 1]  
    negative_samples = freeDf[freeDf['target'] == 0].sample(frac=0.1, random_state=0)  
        
    # 合并进行采样后的数据  
    sampled_df = pd.concat([positive_samples, negative_samples])  
    
    # 选取特征列  
    X = sampled_df[['heroLevelUp', 'appLaunch', 'login', 'payAction', 'plunder', 'goldCost', 'onlineTime', 'mainLevel', 'radar']]  
        
    # 重新选取目标列  
    y = sampled_df['target']  
        
    # 将数据集分为训练集和测试集  
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)  
  
    # 定义随机森林分类器，使用class_weight来增强偏向查全率  
    rf_clf = RandomForestClassifier(n_estimators=100, random_state=0, class_weight={0: 1, 1: 10}, n_jobs=-1)  
  
    # 定义网格搜索以优化模型参数  
    param_grid = {  
        'n_estimators': [100, 200],  
        'max_depth': [10, 20, None],  
        'min_samples_split': [2, 10],  
        'min_samples_leaf': [1, 4],  
    }  
  
    # 使用查全率作为主要评分标准  
    scorer = make_scorer(recall_score)  
  
    grid_search = GridSearchCV(rf_clf, param_grid, scoring=scorer, cv=5)  
    grid_search.fit(X_train, y_train)  
  
    rf_clf_best = grid_search.best_estimator_  
  
    # 在训练集和测试集上进行预测  
    y_train_pred_proba = rf_clf_best.predict_proba(X_train)[:, 1]  
    y_train_pred = (y_train_pred_proba > 0.3).astype(int)  
  
    y_test_pred_proba = rf_clf_best.predict_proba(X_test)[:, 1]  
    y_test_pred = (y_test_pred_proba > 0.3).astype(int)  
  
    # 输出训练集表现  
    print("Training Performance:")  
    print(classification_report(y_train, y_train_pred))  
    print('Training Recall:', recall_score(y_train, y_train_pred))  
  
    # 输出测试集表现  
    print("Test Performance:")  
    print(classification_report(y_test, y_test_pred))  
    print('Test Recall:', recall_score(y_test, y_test_pred))  
      
# 运行函数
# classify_and_evaluate()
