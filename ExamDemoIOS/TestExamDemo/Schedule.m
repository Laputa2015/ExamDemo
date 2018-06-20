//
//  Schedule.m
//  ExamDemo
//
//  Created by George She on 2018/6/8.
//  Copyright © 2018年 CMRead. All rights reserved.
//

#import "Schedule.h"
#import "ReturnCodeKeys.h"

@interface Task:NSObject
@property(nonatomic,assign) int taskId;
@property(nonatomic,assign) int consumption;
@end;

@implementation Task
- (instancetype)initWithId:(int)tID Consumption:(int)consumption
{
    self = [super init];
    if (self) {
        self.taskId = tID;
        self.consumption = consumption;
    }
    return self;
}
@end;

@interface Node:NSObject
@property(nonatomic,assign) int nodeId;
@property(nonatomic,assign) int consumption;
@end

@implementation Node
- (instancetype)initWithId:(int)nID Consumption:(int)consumption
{
    self = [super init];
    if (self) {
        self.nodeId = nID;
        self.consumption = consumption;
    }
    return self;
}
@end;

@interface Schedule()
@property(nonatomic,strong) NSMutableArray *tasks;//任务挂起队列
@property(nonatomic,strong) NSMutableArray *nodes;//服务器节点
@property(nonatomic,strong) NSMutableArray *taskInfos;//任务状态
@end;

@implementation Schedule
- (instancetype)init
{
    self = [super init];
    if (self) {
        [self clean];
    }
    return self;
}

-(int)clean{
    [self.tasks removeAllObjects];
    [self.nodes removeAllObjects];
    [self.taskInfos removeAllObjects];
    self.tasks = nil;
    self.nodes = nil;
    self.taskInfos = nil;
    self.tasks = [[NSMutableArray alloc] initWithCapacity:0];
    self.nodes = [[NSMutableArray alloc] initWithCapacity:0];
    self.taskInfos = [[NSMutableArray alloc] initWithCapacity:0];
    return kE001;//初始化成功
}

-(int)registerNode:(int)nodeId{
    if (nodeId <=0) {
        return kE004;//服务节点编号非法
    }
    
    for (Node *node in self.nodes) {
        if (node.nodeId == nodeId) {
            return kE005;//服务节点已注册
        }
    }
    
    Node *node = [[Node alloc] initWithId:nodeId Consumption:0];
    [self.nodes addObject:node];
    return kE003;//服务节点注册成功
}

-(int)unregisterNode:(int)nodeId{
    if (nodeId <=0) {
        return kE004;//服务节点编号非法
    }
    for (Node *item in self.nodes) {
        if (item.nodeId == nodeId) {
            [self.nodes removeObject:item];
            return kE006;//服务节点注销成功
        }
    }
    return kE007;//服务节点不存在
}

-(int)addTask:(int)taskId withConsumption:(int)consumption{
    if (taskId <=0) {
        return kE009;//任务编号非法
    }
    for (Task *task in self.tasks) {
        if (task.taskId == taskId) {
            return kE010;//任务已添加
        }
    }
    Task *task = [[Task alloc] initWithId:taskId Consumption:consumption];
    [self.tasks addObject:task];
    return kE008;//任务添加成功
}

-(int)deleteTask:(int)taskId{
    if (taskId <=0) {
        return kE009;//任务编号非法
    }
    
    int tId = -1;
    for (Task *item in self.tasks) {
        if (item.taskId == taskId) {
            [self.tasks removeObject:item];
            tId = taskId;
            return kE011;//任务删除成功
        }
    }
    return kE012;//任务不存在
}

-(int)scheduleTask:(int)threshold{
    if(threshold <= 0){
        return kE002;//调度阈值非法
    }
    
    NSArray *sortTaskArray = [self selectionSortArray:self.tasks ByAsc:NO];//降序
    NSArray *sortNodeArray = [self selectionSortArray:self.nodes ByAsc:YES];//升序
    
    //映射关系[task,node]
    for (Task *task in sortTaskArray) {
        Node *minNode = [self nodeWithMinConsumptionInArray:sortNodeArray];
        minNode.consumption += task.consumption;
        TaskInfo *tInfo = [[TaskInfo alloc] init];
        tInfo.nodeId = minNode.nodeId;
        tInfo.taskId = task.taskId;
        [self.taskInfos addObject:tInfo];
    }
    
    for (int i=0; i<self.nodes.count-1; i++) {
        int nodeThreadhold = abs([self.nodes[i] consumption] - [self.nodes[i+1] consumption]);
        if (nodeThreadhold >threshold) {
            return kE014;//无合适迁移方案
        }
    }
    
    //按照task进行升序排序
    [self selectionSortArray:self.taskInfos ByAsc:YES];
    
    //task最小序排列
    for (int i=0; i<self.taskInfos.count-1; i++) {
        int nodeIdi = [self.taskInfos[i] nodeId];
        int taskIdi = [self.taskInfos[i] taskId];
        for (int j=i+1; j<self.taskInfos.count; j++) {
            int nodeIdj = [self.taskInfos[j] nodeId];
            int taskIdj = [self.taskInfos[j] taskId];
            Task *taski = [self taskWithId:taskIdi];
            Task *taskj = [self taskWithId:taskIdj];
            if ([taski consumption] == [taskj consumption] &&
                nodeIdi >nodeIdj) {
                [self.taskInfos[i] setNodeId:nodeIdj];
                [self.taskInfos[j] setNodeId:nodeIdi];
            }
        }
    }
    return kE013;//任务调度成功
}

-(int)queryTaskStatus:(NSMutableArray<TaskInfo *> *)tasks
{
    if(!tasks){
        return kE016;//参数列表非法
    }
    [tasks addObjectsFromArray:self.taskInfos];
    return kE015;//查询任务状态成功
}

//获取task元素
- (Task*)taskWithId:(int)taskId
{
    for (Task *task in self.tasks) {
        if (task.taskId == taskId) {
            return task;
        }
    }
    return nil;
}

//获取node元素
- (Node*)nodeWithId:(int)nodeId
{
    for (Node *node in self.nodes) {
        if (node.nodeId == nodeId) {
            return node;
        }
    }
    return nil;
}

//获取消耗率最小的服务器节点
- (Node*)nodeWithMinConsumptionInArray:(NSArray<id>*)array
{
    int k = 0;
    for (int i=1; i<array.count; i++) {
        if ([array[i] consumption] < [array[k] consumption] ) {
            k = i;
        }
    }
    return array[k];
}

//元素比较
- (BOOL)cmpObj1:(id)obj1 WithObj2:(id)obj2 ByAsc:(BOOL)bAsc
{
    if ([obj1 isKindOfClass:[Task class]]) {
        if (bAsc) {
            //升序
            return [obj1 consumption] < [obj2 consumption];
        }else{
            //降序
            return [obj1 consumption] > [obj2 consumption];
        }
    }else if([obj1 isKindOfClass:[Node class]]){
        if (bAsc) {
            return [obj1 nodeId] < [obj2 nodeId];
        }else{
            return [obj1 nodeId] > [obj2 nodeId];
        }
    }else if([obj1 isKindOfClass:[TaskInfo class]]){
        if(bAsc){
            return [obj1 taskId] < [obj2 taskId];
        }else{
            return [obj1 taskId] > [obj2 taskId];
        }
    }
    return kE000;
}

//数组从大到小排序(简单选择排序)
- (NSArray *)selectionSortArray:(NSMutableArray<id>*)array ByAsc:(BOOL)bAsc
{
    int k = 0;
    NSUInteger count = array.count;
    for (int i=0; i<count-1; i++) {
        k = i;//初始值
        for (int j=i+1; j<count; j++) {
            if ([self cmpObj1:array[j] WithObj2:array[k] ByAsc:bAsc]) {
                k = j;
            }
        }
        //交换次序
        if (k != i) {
            Task *tmp = array[i];
            array[i] = array[k];
            array[k] = tmp;
        }
    }
    return array;
}

@end
