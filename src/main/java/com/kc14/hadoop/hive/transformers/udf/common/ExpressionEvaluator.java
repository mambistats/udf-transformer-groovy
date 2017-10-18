package com.kc14.hadoop.hive.transformers.udf.common;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

public interface ExpressionEvaluator {

	public ScriptEngine getScriptEngine();
	public Object eval() throws ScriptException;

}
