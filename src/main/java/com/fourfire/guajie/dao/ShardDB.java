package com.fourfire.guajie.dao;

import org.apache.commons.lang3.StringUtils;

import com.fourfire.guajie.service.ShardeDBBLL.DATATYPE;

/**共享模型*/
public class ShardDB {
	private String tablename;// 表名称
	private String identityKey;// 自增主键,没有就为null

	private String shardCol;// 分表字段
	private DATATYPE shardColType;// 分表字段类型

	// 依据这个字段来删除分表的,历史数据
	// 默认的 delCol = shardCol
	private String delCol;

	/**********************************/
	public ShardDB(String tablename) {
		this.tablename = tablename;
	}

	public ShardDB(String tablename, String identityKey) {
		this.tablename = tablename;
		this.identityKey = identityKey;
	}

	public ShardDB(String tablename, String identityKey, String shardCol,String delCol, DATATYPE shardColType) {
		this.tablename = tablename;
		this.identityKey = identityKey;
		this.shardCol = shardCol;
		this.shardColType = shardColType;
		this.delCol = delCol;
	}

	/**********************************/

	public String getShardCol() {
		if (StringUtils.isBlank(shardCol)) {
			shardCol = "ienddate";
		}
		return shardCol;
	}

	public String getDelCol() {
		if (StringUtils.isBlank(delCol)) {
			delCol = getShardCol();
		}
		return delCol;
	}

	public void setDelCol(String delCol) {
		this.delCol = delCol;
	}

	public DATATYPE getShardColType() {
		if (shardColType == null) {
			shardColType = DATATYPE.YYYYMM;
		}
		return shardColType;
	}

	public void setShardCol(String shardCol) {
		this.shardCol = shardCol;
	}

	public void setShardColType(DATATYPE shardColType) {
		this.shardColType = shardColType;
	}

	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public String getIdentityKey() {
		return identityKey;
	}

	public void setIdentityKey(String identityKey) {
		this.identityKey = identityKey;
	}

}
