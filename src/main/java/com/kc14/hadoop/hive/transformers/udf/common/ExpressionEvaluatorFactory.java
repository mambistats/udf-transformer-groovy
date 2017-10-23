package com.kc14.hadoop.hive.transformers.udf.common;

import javax.script.Compilable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class ExpressionEvaluatorFactory {

	public static ExpressionEvaluator createExpressionEvaluator (String scriptLanguage, String expr) throws ScriptException {
		ScriptEngineManager engineFactory = new ScriptEngineManager();
		ScriptEngine scriptEngine = engineFactory.getEngineByName(scriptLanguage);
		if (scriptEngine instanceof Compilable) return new CompilableExpressionEvaluator(scriptEngine, expr);
		else return new StandardExpressionEvaluator(scriptEngine, expr);
	}
	
}
