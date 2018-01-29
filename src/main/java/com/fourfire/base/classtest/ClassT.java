package com.fourfire.base.classtest;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Date;

public class ClassT {

	public static void main(String[] args) {
		X x = new X();
		addObjectField(x);
		System.out.println(x);
	}

	/**
	 * 
	 * <ul>
	 * <li>方法名：  addObjectField </li>
	 * <li>功能描述：类中属性值==null,就补充一个默认值 </li>
	 * <li>创建人：  jy </li>
	 * <li>创建时间：2017年8月16日 </li>
	 * </ul> 
	 * @param <T>
	 * @param model
	 */
	public static <T> T addObjectField(T model) {
		Field[] field = model.getClass().getDeclaredFields(); // 获取实体类的所有属性，返回Field数组
		try {
			for (int j = 0; j < field.length; j++) { // 遍历所有属性
				String name = field[j].getName(); // 获取属性的名字

				String type = field[j].getGenericType().toString(); // 获取属性的类型

				System.out.println(name + "  " + type);
				if (name.equalsIgnoreCase("serialVersionUID") && type.equals("long")) {
					continue;
				}
				name = name.substring(0, 1).toUpperCase() + name.substring(1); // 将属性的首字符大写，方便构造get，set方法
				if (type.equals("class java.lang.String")) { // 如果type是类类型，则前面包含"class "，后面跟类名
					Method m = model.getClass().getMethod("get" + name);
					String value = (String) m.invoke(model); // 调用getter方法获取属性值
					if (value == null) {
						m = model.getClass().getMethod("set" + name, String.class);
						m.invoke(model, "");
					}
				} else if (type.equals("class java.lang.Integer") || type.equals("int")) {
					Method m = model.getClass().getMethod("get" + name);
					Integer value = (Integer) m.invoke(model);
					if (value == null) {
						m = model.getClass().getMethod("set" + name, Integer.class);
						m.invoke(model, 0);
					}
				} else if (type.equals("class java.lang.Boolean")) {
					Method m = model.getClass().getMethod("get" + name);
					Boolean value = (Boolean) m.invoke(model);
					if (value == null) {
						m = model.getClass().getMethod("set" + name, Boolean.class);
						m.invoke(model, false);
					}
				} else if (type.equals("class java.util.Date")) {
					Method m = model.getClass().getMethod("get" + name);
					Date value = (Date) m.invoke(model);
					if (value == null) {
						m = model.getClass().getMethod("set" + name, Date.class);
						m.invoke(model, new Date());
					}
				} else if (type.equals("class java.lang.Long") || type.equals("long")) {
					Method m = model.getClass().getMethod("get" + name);
					Long value = (Long) m.invoke(model);
					if (value == null) {
						m = model.getClass().getMethod("set" + name, Long.class);
						m.invoke(model, 0L);
					}
				} else if (type.equals("class java.lang.Double") || type.equals("double")) {
					Method m = model.getClass().getMethod("get" + name);
					Double value = (Double) m.invoke(model);
					if (value == null) {
						m = model.getClass().getMethod("set" + name, Double.class);
						m.invoke(model, 0D);
					}
				} else if (type.equals("class java.math.BigDecimal")) {
					Method m = model.getClass().getMethod("get" + name);
					BigDecimal value = (BigDecimal) m.invoke(model);
					if (value == null) {
						m = model.getClass().getMethod("set" + name, BigDecimal.class);
						m.invoke(model, new BigDecimal(0));
					}
				} else if (type.equals("class java.lang.Number")) {
					Method m = model.getClass().getMethod("get" + name);
					Number value = (Number) m.invoke(model);
					if (value == null) {
						m = model.getClass().getMethod("set" + name, Number.class);
						m.invoke(model, 0);
					}
				} else {
					System.out.println("-自己补充缺少类型---------------" + type);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return model;
	}
}
