## 功能列表

### 注册
注册，手机号注册，第三方注册（比如google账号）
注册需要验证码
新用户个人信息填写，名字，性别，头像等

### 卡信息
总体余额，欠款等信息
罗列所有已绑定信用卡，包括开卡银行，卡号，余额等
绑定新的信用卡功能
罗列所有已绑定借记卡，包括开卡银行，卡号，余额等
绑定新借记卡功能
账单的汇总信息，要单独做一个界面
账单的详细信息，要单独做一个界面，每一张信用卡单独详细信息

### 添加卡功能
手动输入卡信息
扫描卡照片
通过pdf（暂时不太明白）
通过gmail（暂时不太明白）
输入之后应该需要后端进行确认，确认之后才能绑定（银行接口不确认）

### 添加账单
通过邮箱，选择不同的邮件服务商，填入邮箱信息
邮箱需要，授权。授权完成之后，后端会定时获取账单。也应该有手动获取账单的功能。这里应该有时间限制，比如获取最近多久范围内账单，邮件也只针对这段时间）
通过文件上传。上传文件可以有密码（具体上传格式和密码暂时未确定）
通过手动记账。此功能过于繁复是否为核心功能？

### 账单
智能报表，即一个用户可以比较快速查看最近流水的简单界面
智能报表-历史，上面功能的较远可选时间段的版本
数据可视化，钱都花在什么方向，服装，吃饭等。还可以做出更多的可视化效果。这个可能不是核心功能。
上述可视化效果的历史版本，即可选时间段的版本

### 我的
个人信息，应该可以收，并记录系统给发的信息或者奖励

### 奖励
商城等功能，可以兑换奖励，比如兑换优惠券，兑换礼品等。应该是非核心功能

## 需求
将上述功能切分，组成3期内容。第一期和第二期应该是核心功能，第三期是其他功能。第一期应该偏向于框架搭建，第二期完成应该就可以进行核心功能的循环。第三期算是润色。

上述是要做一个跨平台的app（安卓，iOS）。前端后端都要开发。

## 计划
项目目标：开发一个跨平台的移动应用，用于管理用户的信用卡和借记卡信息，提供账单查询和数据可视化功能。

项目团队：

前端开发工程师：1人，负责前端界面开发与交互功能
后端开发工程师：2人，负责API开发、数据库设计及后端系统的扩展性和稳定性
UI/UX设计师：1人，负责界面设计与用户体验优化
项目经理/测试负责人：1人，负责项目管理、协调与测试
技术选型： 前端：React Native（跨平台框架，可同时开发Android和iOS应用） 后端：Node.js（使用Express框架）或Golang（使用Gin框架） 数据库：MongoDB 或者 MySQL

注意事项：

与银行和金融机构保持紧密沟通，了解他们的API接口、安全规范和数据保护要求，确保应用的合规性。
选择合适的短信服务商，了解他们的API接口以及如何接入。
熟悉第三方注册平台（如Google账号）的API接口，确保顺利接入。
了解不同邮件服务商（如Gmail、Outlook等）的API接口，以实现通过邮箱获取账单的功能。
在开发过程中，确保应用能够应对较多用户的压力，实现分布式架构或其他动态扩容方案。
项目阶段：

预研阶段：

确定合作银行及其API接口
选择短信服务商及其API接口
了解第三方注册平台（如Google账号）的API接口
确定邮箱服务商的API接口 预研阶段时间：2周
第一期（核心功能与框架搭建）：

注册功能（手机号注册，第三方注册）
个人信息填写
卡信息展示（信用卡与借记卡）
绑定新卡功能（信用卡与借记卡）
账单汇总与详细信息
添加账单（通过邮箱，文件上传，手动记账） 第一期时间：4周
第二期（核心功能完善）：

智能报表（最近流水与历史）
数据可视化（消费方向等）
我的（个人信息，系统消息）
优化用户体验和性能 第二期时间：3周
第三期（附加功能）：

奖励（商城，兑换优惠券，礼品等）
更多的数据可视化效果
其他润色功能 第三期时间：2周
总共时间：11周，约2个月半至3个月。