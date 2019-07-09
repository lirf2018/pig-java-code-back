-- h5日志生成中间数据和静态数据

---------资讯类中间数据
--加载资讯类别关联数据（资讯Id`资讯名称`类别id`类别名称）
allData_static_zt = load '$input_static_article_data' using PigStorage('`') As (articleId:chararray,articleName:chararray,typeId:chararray,typeName:chararray);
fch_allData_static_zt = foreach allData_static_zt generate articleId,typeId;
dis_fch_allData_static_zt = distinct fch_allData_static_zt;
grp_dis_fch_allData_static_zt = group dis_fch_allData_static_zt by articleId;
store grp_dis_fch_allData_static_zt into '$out_article_type_group_data';

---------卡券类中间数据
--加载卡券关联数据（卡券Id`卡券名称`库存`开始时间`结束时间`类型id`类型名称）
allData_static_tt = load '$input_static_ticket_data' using PigStorage('`') As (ticketId:chararray,ticketName:chararray,kc:chararray,startTime:chararray,endTime:chararray,typeId:chararray,typeName:chararray);
--处理中间数据（卡券类型分组）------------非简单数据
fch_allData_static_tt = foreach allData_static_tt generate ticketId,typeId;
dis_fch_allData_static_tt = distinct fch_allData_static_tt;
grp_dis_fch_allData_static_tt = group dis_fch_allData_static_tt by ticketId;
store grp_dis_fch_allData_static_tt into '$out_ticket_type_group_data';


--------- 淘流量中间数据
--加载任务类型属性(任务Id`任务名称`类型id`类型名称`类型编码`属性id`属性名称)
allData_static_tasktp = load '$input_static_tasktypeproperty_data' using PigStorage('`') As (taskId:chararray,taskName:chararray,typeId:chararray,typeName:chararray,typeCode:chararray,pId:chararray,pName:chararray);
--任务类型(任务类型分组)------------非简单数据
fch_allData_static_tasktype = foreach allData_static_tasktp generate taskId,typeId;
dis_fch_allData_static_tasktype = distinct fch_allData_static_tasktype;
grp_dis_fch_allData_static_tasktype = group dis_fch_allData_static_tasktype by taskId;
store grp_dis_fch_allData_static_tasktype into '$output_static_tasktype_data';
--任务属性（任务属性分组）------------非简单数据
fch_allData_static_taskproperty = foreach allData_static_tasktp generate taskId,pId;
dis_fch_allData_static_taskproperty = distinct fch_allData_static_taskproperty;
grp_dis_fch_allData_static_taskproperty = group dis_fch_allData_static_taskproperty by taskId;
store grp_dis_fch_allData_static_taskproperty into '$output_static_taskproperty_data';

-- 加载任务类别 (任务Id`任务名称`类别id`类别名称)
allData_static_taskc = load '$input_static_taskcategory_data' using PigStorage('`') As (taskId:chararray,taskName:chararray,cId:chararray,cName:chararray);
fch_allData_static_taskc =  foreach allData_static_taskc generate taskId,cId;
dis_fch_allData_static_taskc = distinct fch_allData_static_taskc;
grp_dis_fch_allData_static_taskc = group dis_fch_allData_static_taskc by taskId;
store grp_dis_fch_allData_static_taskc into '$output_static_taskcategory_data';



----------------------------从关系中得到的静态表数据
----得到类型表
fch_task_type = foreach allData_static_tasktp generate typeId,typeCode,typeName;
dis_fch_task_type = distinct fch_task_type;
fl_dis_fch_task_type = filter dis_fch_task_type by (typeName != 'None' and typeName is not null);

store fl_dis_fch_task_type into '$output_static_type_data' using PigStorage('`');

----得到属性表
fch_task_property = foreach allData_static_tasktp generate pId,pName;
dis_fch_task_property = distinct fch_task_property;
fl_dis_fch_task_property = filter dis_fch_task_property by (pName != 'None' and pName is not null);

store fl_dis_fch_task_property into '$output_static_property_data' using PigStorage('`');
















