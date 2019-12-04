create 't_user_info','base_info','extra_info'
put 't_user_info','001','base_info:username','gugu1'
put 't_user_info','001','base_info:sex','female'
put 't_user_info','001','extra_info:career','it'
put 't_user_info','001','extra_info:addr','shanghai'
scan 't_user_info'
delete 't_user_info','001','extra_info:addr'
-- 删除一行
deleteall 't_user_info','001'
-- 删除表
disable 't_user_info'
-- enable 't_user_info'
drop 't_user_info'