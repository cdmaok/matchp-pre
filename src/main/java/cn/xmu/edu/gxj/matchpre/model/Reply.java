package cn.xmu.edu.gxj.matchpre.model;

import cn.xmu.edu.gxj.matchpre.util.MPException;

public class Reply {

	private String Message;
	private int Code;
	
	public Reply(MPException e){
		this.Message = e.getMessage();
		this.Code = e.getErrCode();
	}
	
	public Reply(String message,int code){
		this.Message = message;
		this.Code = code;
	}
	
	public String getMessage() {
		return Message;
	}
	public void setMessage(String message) {
		Message = message;
	}
	public int getCode() {
		return Code;
	}
	public void setCode(int code) {
		Code = code;
	}
	

}
