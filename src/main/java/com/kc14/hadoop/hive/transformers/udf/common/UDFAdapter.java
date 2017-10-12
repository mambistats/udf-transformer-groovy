package com.kc14.hadoop.hive.transformers.udf.common;

import javax.script.ScriptEngine;

public abstract class UDFAdapter implements UDFPackageIF {
	
	@Override
	public void putInto(ScriptEngine engine) {
		engine.put(this.getPackageName(), this);
	}

	@Override
	public void prepareInputRow(String[] inputRow) {}

}
