drop table if exists aqopiapp.t_setting;

create table aqopiapp.t_setting(
  setting_id int not null auto_increment
, created_date datetime not null default current_timestamp
, setting_key varchar(255) not null 
, setting_value varchar(255) not null
, primary key (setting_id)
);

delimiter //

drop procedure if exists aqopiapp.p_set_setting//

create procedure aqopiapp.p_set_setting(
    a_setting_key varchar(255)
  , a_setting_value varchar(255)
)
begin
  declare v_setting_id int;
    
  select setting_id
    from t_setting 
   where setting_key = a_setting_key
    into v_setting_id;
     
  if (v_setting_id is null)
    then 
      insert
        into t_setting 
           ( setting_key
           , setting_value
           )
      select a_setting_key
           , a_setting_value;
    else 
      update t_setting 
         set setting_value = a_setting_value
       where setting_id = v_setting_id;
    end if; 
end//

delimiter ;

call aqopiapp.p_set_setting('MBD_REFRESHED', '2019-03-01');
call aqopiapp.p_set_setting('MOH_REFRESHED', '2019-03-01');
call aqopiapp.p_set_setting('UWV_REFRESHED', '2020-01-01');
call aqopiapp.p_set_setting('MPO_REFRESHED', '2019-03-01');
