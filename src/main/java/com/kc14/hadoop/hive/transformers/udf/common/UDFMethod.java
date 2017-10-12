package com.kc14.hadoop.hive.transformers.udf.common;

import java.lang.reflect.Method;

public class UDFMethod {
	
	public UDFPackageIF udfPackage;
	public Method       udfMethod;
	
	public UDFMethod(UDFPackageIF udfPackage, Method udfMethod) {
		super();
		this.udfPackage = udfPackage;
		this.udfMethod = udfMethod;
	}

}
