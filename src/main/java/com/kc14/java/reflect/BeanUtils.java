package com.kc14.java.reflect;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class BeanUtils {

	public static Object[] NO_ARGUMENTS_ARRAY = new Object[] {};
	
	public static Map<String, String> getBeanProperties(Object bean) {
	    Map<String, String> properties = null;
		try {
		    BeanInfo beanIntrospector = Introspector.getBeanInfo (bean.getClass());
		    PropertyDescriptor[] propertyDescriptors = beanIntrospector.getPropertyDescriptors();
		    properties = new HashMap<String, String>(propertyDescriptors.length);
		    for (PropertyDescriptor propertyDescriptor: propertyDescriptors) {
		        String propertyName = propertyDescriptor.getName();
		        // if (propertyName.equalsIgnoreCase("class"))
		        Method readMethod = propertyDescriptor.getReadMethod();
		        Object value = readMethod.invoke(bean, NO_ARGUMENTS_ARRAY);
		        if (value == null) properties.put(propertyName, null);
		        else properties.put(propertyName, value.toString());
		    }
		} catch (java.beans.IntrospectionException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return properties;
	}
	
}
