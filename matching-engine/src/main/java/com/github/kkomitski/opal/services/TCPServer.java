package com.github.kkomitski.opal.services;

import com.github.kkomitski.opal.OrderBook;
import com.github.kkomitski.opal.orderbook.OrderRequest;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.FixedLengthFrameDecoder;

public class TCPServer {
  private static final int PORT = 42069;
  private static final int BUFFER_SIZE = 65535;

  public void startServer(OrderBook[] orderBooks) throws Exception {
    int numWorkerThreads = Runtime.getRuntime().availableProcessors() * 4;
    MultiThreadIoEventLoopGroup bossGroup = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    MultiThreadIoEventLoopGroup workerGroup = new MultiThreadIoEventLoopGroup(numWorkerThreads,
        NioIoHandler.newFactory());

    try {
      ServerBootstrap bootstrap = new ServerBootstrap();
      bootstrap.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, 128)
          .option(ChannelOption.SO_REUSEADDR, true)
          .childOption(ChannelOption.SO_KEEPALIVE, true)
          .childOption(ChannelOption.TCP_NODELAY, true)
          .childOption(ChannelOption.SO_RCVBUF, BUFFER_SIZE) // receive buffer
          .childOption(ChannelOption.SO_SNDBUF, BUFFER_SIZE) // send buffer
          .childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) {
              ch.pipeline().addLast(new FixedLengthFrameDecoder(OrderRequest.REQUEST_SIZE)); // Add frame decoder
              ch.pipeline().addLast(new OrderHandler(orderBooks));
            }
          });
      ChannelFuture future = bootstrap.bind(PORT).sync();
      System.out.println("Matching Engine listening on port " + PORT);

      System.out.println("\n\n\u001B[32m" +
          "##########################\n" +
          "#          HOT           #\n" +
          "##########################\n" +
          "\u001B[0m\n\n");

      future.channel().closeFuture().sync();
    } finally {
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
    }
  }

  private static class OrderHandler extends ChannelInboundHandlerAdapter {
    private final OrderBook[] orderBooks;

    public OrderHandler(OrderBook[] orderBooks) {
      this.orderBooks = orderBooks;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      ByteBuf buf = (ByteBuf) msg;
      try {
        // Read instrument index first to select the order book
        int instrumentIndex = buf.getShort(0) & 0x7FFF; // Assuming big-endian, adjust if needed
        if (instrumentIndex >= 0 && instrumentIndex < orderBooks.length) {
          orderBooks[instrumentIndex].publishOrder(buf);
        } else {
          System.err.println("Invalid instrument index: " + instrumentIndex);
        }
      } finally {
        buf.release();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      cause.printStackTrace();
      ctx.close();
    }
  }
}