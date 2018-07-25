package datanode.rpc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import net.transfer.RPCInfo;

public class DataNodeReportRpc {
	private String ip;
	private int port;

	public DataNodeReportRpc(String ip, int port) {
		super();
		this.ip = ip;
		this.port = port;
	}

	@SuppressWarnings("unchecked")
	public <T> T create(Object target) {

		return (T) Proxy.newProxyInstance(target.getClass().getClassLoader(), target.getClass().getInterfaces(),
				new InvocationHandler() {

					@Override
					public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

						RPCInfo rpcInfo = new RPCInfo();
						rpcInfo.setClassName(target.getClass().getName());
						rpcInfo.setMethodName(method.getName());
						rpcInfo.setObjects(args);
						rpcInfo.setTypes(method.getParameterTypes());

						ResultHandler resultHandler = new ResultHandler();
						EventLoopGroup group = new NioEventLoopGroup();
						try {
							Bootstrap b = new Bootstrap();
							b.group(group).channel(NioSocketChannel.class).option(ChannelOption.TCP_NODELAY, true)
									.handler(new ChannelInitializer<SocketChannel>() {
										@Override
										public void initChannel(SocketChannel ch) throws Exception {
											ChannelPipeline pipeline = ch.pipeline();
											pipeline.addLast("frameDecoder",
													new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
											pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
											pipeline.addLast("encoder", new ObjectEncoder());
											pipeline.addLast("decoder", new ObjectDecoder(Integer.MAX_VALUE,
													ClassResolvers.cacheDisabled(null)));
											pipeline.addLast("handler", resultHandler);
										}
									});

							ChannelFuture future = b.connect(ip, port).sync();
							future.channel().writeAndFlush(rpcInfo).sync();
							future.channel().closeFuture().sync();
						} finally {
							group.shutdownGracefully();
						}
						return resultHandler.getResponse();
					}
				});
	}
}
