package client.rpc;

import org.apache.log4j.Logger;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ResultHandler extends ChannelInboundHandlerAdapter {
	 
	private Object response;  
    
	private static Logger logger = Logger.getLogger(ResultHandler.class);
    public Object getResponse() {  
    return response;  
}  
 
    @Override  
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {  
        response=msg;  
        logger.info("client接收到服务器返回的消息:" + msg);  
    }  
      
    @Override  
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {  
    	logger.info("client exception is general");  
    }  
}
