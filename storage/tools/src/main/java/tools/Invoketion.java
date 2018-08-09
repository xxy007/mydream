package tools;

import java.io.Serializable;
import java.util.Arrays;

public 	class Invoketion implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7756428349931696456L;
	private String methodName;
	private Class<?>[] parameterTypes;
	private Object[] arguments ;
	public Invoketion() {
	}
	public String getMethodName() {
		return methodName;
	}
	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}
	public Class<?>[] getParameterTypes() {
		return parameterTypes;
	}
	public void setParameterTypes(Class<?>[] parameterTypes) {
		this.parameterTypes = parameterTypes;
	}
	public Object[] getArguments() {
		return arguments;
	}
	public void setArguments(Object[] arguments) {
		this.arguments = arguments;
	}
	@Override
	public String toString() {
		return "invoketion [methodName=" + methodName + ", parameterTypes=" + Arrays.toString(parameterTypes)
				+ ", arguments=" + Arrays.toString(arguments) + "]";
	}
}