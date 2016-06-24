package cn.xmu.edu.gxj.matchpre.util;

public class MPException extends Exception{
	private String Message;
	private int ErrCode;
	
	public MPException(String Message,int ErrCode) {
		super();
		this.Message = Message;
		this.ErrCode = ErrCode;
	}
	
	public MPException(int ErrCode,String Message){
		super();
		this.Message = Message;
		this.ErrCode = ErrCode;
	}
	
	public String getMessage() {
		return Message;
	}
	public void setMessage(String message) {
		Message = message;
	}
	public int getErrCode() {
		return ErrCode;
	}
	public void setErrCode(int errCode) {
		ErrCode = errCode;
	}
	
}
