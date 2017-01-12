DROP TABLE IF  EXISTS lianjia_fang_regions ;
CREATE TABLE IF NOT EXISTS  lianjia_fang_regions (
  fang_regions varchar(100) NOT NULL,
  arrangement varchar(100) DEFAULT NULL,
  upper_fang_regions varchar(45) DEFAULT NULL,
  onsale_state varchar(45) DEFAULT NULL,
  trans_state varchar(45) DEFAULT NULL,
  city varchar(45) DEFAULT NULL,
  created_date datetime DEFAULT CURRENT_TIMESTAMP,
  updated_date datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (fang_regions)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF  EXISTS lianjia_fang_transaction ;
CREATE TABLE IF NOT EXISTS lianjia_fang_transaction (
  transaction_date varchar(45) DEFAULT NULL,
  fang_key varchar(45) NOT NULL,
  fang_desc varchar(45) DEFAULT NULL,
  fang_url varchar(200) DEFAULT NULL,
  price varchar(45) DEFAULT NULL,
  price_pre varchar(45) DEFAULT NULL,
  xiaoqu varchar(45) DEFAULT NULL,
  huxing varchar(45) DEFAULT NULL,
  mianji varchar(45) DEFAULT NULL,
  quyu varchar(45) DEFAULT NULL,
  bankuai varchar(45) DEFAULT NULL,
  louceng varchar(45) DEFAULT NULL,
  chaoxiang varchar(45) DEFAULT NULL,
  age varchar(45) DEFAULT NULL,
  subway varchar(45) DEFAULT NULL,
  taxfree varchar(45) DEFAULT NULL,
  haskey varchar(45) DEFAULT NULL,
  col_look varchar(45) DEFAULT NULL,
  zhuangxiu varchar(45) DEFAULT NULL,
  city varchar(45) DEFAULT NULL,
  created_date datetime DEFAULT CURRENT_TIMESTAMP,
  updated_date datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (fang_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF  EXISTS lianjia_fang_list ;
CREATE TABLE IF NOT EXISTS lianjia_fang_list (
  fang_key varchar(45) NOT NULL,
  fang_desc varchar(45) DEFAULT NULL,
  fang_url varchar(200) DEFAULT NULL,
  price varchar(45) DEFAULT NULL,
  price_pre varchar(45) DEFAULT NULL,
  xiaoqu varchar(45) DEFAULT NULL,
  huxing varchar(45) DEFAULT NULL,
  mianji varchar(45) DEFAULT NULL,
  quyu varchar(45) DEFAULT NULL,
  bankuai varchar(45) DEFAULT NULL,
  louceng varchar(45) DEFAULT NULL,
  chaoxiang varchar(45) DEFAULT NULL,
  age varchar(45) DEFAULT NULL,
  subway varchar(45) DEFAULT NULL,
  taxfree varchar(45) DEFAULT NULL,
  haskey varchar(45) DEFAULT NULL,
  col_look varchar(45) DEFAULT NULL,
  address varchar(300) DEFAULT NULL,
  data_source varchar(300) DEFAULT 'Lianjia',
  city varchar(45) DEFAULT NULL,
  created_date datetime DEFAULT CURRENT_TIMESTAMP,
  updated_date datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (fang_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF  EXISTS lianjia_fang_xiaoqu ;
CREATE TABLE IF NOT EXISTS lianjia_fang_xiaoqu (
  xiaoqu_key varchar(45) NOT NULL,
  xiaoqu_name varchar(200) DEFAULT NULL,
  xiaoqu_url varchar(300) DEFAULT NULL,
  quyu varchar(45) DEFAULT NULL,
  bankuai varchar(45) DEFAULT NULL,
  price varchar(45) DEFAULT NULL,
  fang_url varchar(300) DEFAULT NULL,
  age varchar(45) DEFAULT NULL,
  subway varchar(45) DEFAULT NULL,
  onsale_num varchar(45) DEFAULT NULL,
  city varchar(45) DEFAULT NULL,
  created_date datetime DEFAULT CURRENT_TIMESTAMP,
  updated_date datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (xiaoqu_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF  EXISTS soufang_fang_list ;
CREATE TABLE IF NOT EXISTS soufang_fang_list (
  fang_key varchar(45) NOT NULL,
  fang_desc varchar(45) DEFAULT NULL,
  fang_url varchar(200) DEFAULT NULL,
  price varchar(45) DEFAULT NULL,
  price_pre varchar(45) DEFAULT NULL,
  xiaoqu varchar(45) DEFAULT NULL,
  huxing varchar(45) DEFAULT NULL,
  mianji varchar(45) DEFAULT NULL,
  quyu varchar(45) DEFAULT NULL,
  bankuai varchar(45) DEFAULT NULL,
  louceng varchar(45) DEFAULT NULL,
  chaoxiang varchar(45) DEFAULT NULL,
  age varchar(45) DEFAULT NULL,
  subway varchar(45) DEFAULT NULL,
  taxfree varchar(45) DEFAULT NULL,
  haskey varchar(45) DEFAULT NULL,
  col_look varchar(45) DEFAULT NULL,
  address varchar(300) DEFAULT NULL,
  data_source varchar(300) DEFAULT 'Soufang',
  city varchar(45) DEFAULT NULL,
  created_date datetime DEFAULT CURRENT_TIMESTAMP,
  updated_date datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (fang_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

