CREATE table test
(
    vin       string,
    dt        string,
    iswarning string
) row FORMAT DELIMITED FIELDS TERMINATED BY '\t';

insert overwrite table test
values ("0001", "10001", '0'),
       ("0001", "10002", '0'),
       ("0001", "10003", '0'),
       ("0001", "10004", '1'),
       ("0001", "10005", '1'),
       ("0001", "10006", '1'),
       ("0001", "10007", '0'),
       ("0001", "10008", '0'),
       ("0001", "10009", '0');

-- 已经将结果插入进去了

select *
from test;

with seq as
         (
             select vin,
                    dt,
                    iswarning,
                    row_number() over (partition by vin order by cast(dt as bigint) desc) as rk -- 降序排列
             from test
         ),
     end1 as --  如果最新数据没有发生预警,但是前一个发生了预警,发生预警的最后一条数据
         (
             select *
             from seq
             where rk = 2
               and iswarning = '1'
               and '0' in (select t.iswarning from seq as t where t.rk = 1)
         ),
     begin1 as --  本次首次出现预警的前一条数据
         (
             select *
             from (
                      select vin,
                             dt,
                             row_number() over (partition by vin order by cast(dt as bigint) desc) as rk -- 降序排列
                      from test
                      where iswarning = '0'
                  ) as r1
             where r1.rk = '2'
         ),
     fw as (
         select begin1.vin, end1.dt as ee, begin1.dt as bb
         from begin1
                  join end1 on end1.vin = begin1.vin
         where begin1.dt < end1.dt
     )

-- hive中的join只能是等值连接，因此不能使用left semi  join  左半连接

select test.*
from test
         left join fw on test.vin = fw.vin
where test.dt <= fw.ee
  and test.dt > fw.bb