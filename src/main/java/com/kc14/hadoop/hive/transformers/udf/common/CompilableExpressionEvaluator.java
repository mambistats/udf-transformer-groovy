package com.kc14.hadoop.hive.transformers.udf.common;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

public class CompilableExpressionEvaluator implements ExpressionEvaluator {

	private ScriptEngine   engine;
	private CompiledScript compiledExpr;

	public CompilableExpressionEvaluator(ScriptEngine scriptEngine, String expr) throws ScriptException {
		this.engine = scriptEngine;
		Compilable compilingScriptEngine = (Compilable) scriptEngine;
		this.compiledExpr = compilingScriptEngine.compile(expr);
	}

	@Override
	public Object eval() throws ScriptException {
		return this.compiledExpr.eval();
	}

	@Override
	public ScriptEngine getScriptEngine() {
		return this.engine;
	}

}
