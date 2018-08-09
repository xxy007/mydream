package rpc;

import java.lang.reflect.Method;
import org.apache.log4j.Logger;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import net.transfer.RPCInfo;

public class RpcServer extends Rpc implements Runnable{
	private Object instance;
	private String ip;
	private int port;
	private Logger logger = Logger.getLogger(RpcServer.class);
	public RpcServer() {
	}
    
    public RpcServer setInstance(Object instance) {
      this.instance = instance;
      return this;
    }
    
    public RpcServer setBindAddress(String ip) {
      this.ip = ip;
      return this;
    }
    
    public RpcServer setPort(int port) {
      this.port = port;
      return this;
    }
	
	@Override
	public void run() {
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap serverBootstrap = new ServerBootstrap().group(bossGroup, workerGroup)
					.channel(NioServerSocketChannel.class).localAddress(ip, port)
					.childHandler(new ChannelInitializer<SocketChannel>() {

						@Override
						protected void initChannel(SocketChannel ch) throws Exception {
							ChannelPipeline pipeline = ch.pipeline();
							pipeline.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
							pipeline.addLast(new LengthFieldPrepender(4));
							pipeline.addLast("encoder", new ObjectEncoder());
							pipeline.addLast("decoder",
									new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)));
							pipeline.addLast(new InvokerHandler());
						}
					}).option(ChannelOption.SO_BACKLOG, 128).childOption(ChannelOption.SO_KEEPALIVE, true);
			ChannelFuture future = serverBootstrap.bind(port).sync();
			logger.info("Server start listen at " + port);
			future.channel().closeFuture().sync();
		} catch (Exception e) {
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
	}
	class InvokerHandler extends ChannelInboundHandlerAdapter {
		@Override
		public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
			RPCInfo classInfo = (RPCInfo) msg;
			Object claszz = instance;
			Method method = claszz.getClass().getMethod(classInfo.getMethodName(), classInfo.getTypes());
			Object result = method.invoke(claszz, classInfo.getObjects());
			if (result != null) {
				ctx.write(result);
			}
			ctx.flush();
			ctx.close();
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
			cause.printStackTrace();
			ctx.close();
		}
	}
}
