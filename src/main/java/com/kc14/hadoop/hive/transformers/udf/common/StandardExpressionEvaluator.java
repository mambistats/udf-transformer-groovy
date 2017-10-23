package com.kc14.hadoop.hive.transformers.udf.common;

import javax.script.ScriptEngine;
import javax.script.ScriptException;

public class StandardExpressionEvaluator implements ExpressionEvaluator {

	private ScriptEngine scriptEngine;
	private String       expr;

	public StandardExpressionEvaluator(ScriptEngine scriptEngine, String expr) {
		this.scriptEngine = scriptEngine;
		this.expr = expr;
	}

	@Override
	public Object eval() throws ScriptException {
		return this.scriptEngine.eval(this.expr);
	}

	@Override
	public ScriptEngine getScriptEngine() {
		return this.scriptEngine;
	}

}
