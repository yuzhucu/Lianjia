SELECT  a.*,'fang_list' FROM fang.lianjia_fang_regions a  ;
SELECT  a.*,'fang_list' FROM fang.lianjia_fang_list a where a.updated_date>=curdate() order by a.updated_date desc;
SELECT   a.*,'transaction' FROM fang.lianjia_fang_transaction a where a.updated_date>=curdate() order by a.updated_date desc;
SELECT   a.*,'xiaoqu' FROM fang.lianjia_fang_xiaoqu a where a.updated_date>=curdate() order by a.updated_date desc;

SELECT  count(*),'fang_list' FROM fang.lianjia_fang_list a where a.updated_date>=curdate() order by a.updated_date desc;
SELECT   count(*),'transaction' FROM fang.lianjia_fang_transaction a where a.updated_date>=curdate() order by a.updated_date desc;
SELECT   count(*),'xiaoqu' FROM fang.lianjia_fang_xiaoqu a where a.updated_date>=curdate() order by a.updated_date desc;
#and a.xiaoqu_name='宝隆新村'

SELECT  trim(a.quyu),  trim(bankuai),COUNT(*)/20, COUNT(*), 'transaction' FROM fang.lianjia_fang_transaction a GROUP BY TRIM(a.quyu) ,trim(bankuai) ORDER BY COUNT(*) DESC , TRIM(a.quyu);
SELECT  trim(a.quyu),  trim(bankuai),COUNT(*)/20, COUNT(*), 'xiaoqu' FROM fang.lianjia_fang_xiaoqu a GROUP BY TRIM(a.quyu) ,trim(bankuai) ORDER BY COUNT(*) DESC , TRIM(a.quyu);


 
SELECT  trim(a.quyu), count(*)/20,count(*),'fang_list' FROM fang.lianjia_fang_list a group by trim(a.quyu) order by count(*) desc,trim(a.quyu);
SELECT  trim(a.quyu),  COUNT(*)/20, COUNT(*), 'transaction' FROM fang.lianjia_fang_transaction a GROUP BY TRIM(a.quyu) ORDER BY COUNT(*) DESC , TRIM(a.quyu);
SELECT  trim(a.quyu),  COUNT(*)/20, COUNT(*), 'xiaoqu' FROM fang.lianjia_fang_xiaoqu a GROUP BY TRIM(a.quyu) ORDER BY COUNT(*) DESC , TRIM(a.quyu);


SELECT   count(*)/20,count(*),'fang' FROM fang.lianjia_fang_regions a;
SELECT   count(*)/20,count(*),'fang_list' FROM fang.lianjia_fang_list a;
SELECT   count(*)/20,count(*),'transaction' FROM fang.lianjia_fang_transaction a;
SELECT   count(*)/20,count(*),'xiaoqu' FROM fang.lianjia_fang_xiaoqu a;