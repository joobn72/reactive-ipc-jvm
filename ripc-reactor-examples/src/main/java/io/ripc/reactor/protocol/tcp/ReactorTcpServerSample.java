package io.ripc.reactor.protocol.tcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.ripc.protocol.tcp.TcpServer;
import io.ripc.transport.netty4.tcp.Netty4TcpServer;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Streams;

import java.nio.charset.Charset;

/**
 * Created by jbrisbin on 5/28/15.
 */
public class ReactorTcpServerSample {

    public static void main(String... args) throws InterruptedException {
        //TcpServer<ByteBuf, ByteBuf> transport = Netty4TcpServer.<ByteBuf, ByteBuf>create(0);
        TcpServer<Person, Person> transport = Netty4TcpServer.create(0, new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ch.pipeline()
                  .addFirst(new LoggingHandler(LogLevel.DEBUG))
                  .addFirst(new JsonCodec());
            }
        });
        //echo(transport);
        //echoWithQuitCommand(transport);
        echoWithCodec(transport);
    }

    /**
     * Keep echoing until the client goes away.
     */
    private static void echo(TcpServer<ByteBuf, ByteBuf> transport) {
        ReactorTcpServer.create(transport)
                        .start(connection -> {
                            connection.flatMap(inByteBuf -> {
                                String text = "Hello " + inByteBuf.toString(Charset.defaultCharset());
                                ByteBuf outByteBuf = Unpooled.buffer().writeBytes(text.getBytes());
                                return connection.writeWith(Streams.just(outByteBuf));
                            }).consume();
                            return Streams.never();
                        });
    }

    /**
     * Keep echoing until the client sends "quite".
     */
    private static void echoWithQuitCommand(TcpServer<ByteBuf, ByteBuf> transport) {
        ReactorTcpServer.create(transport)
                        .start(connection -> {
                            Promise<Void> promise = Promises.prepare();
                            connection.flatMap(inByteBuf -> {
                                String input = inByteBuf.toString(Charset.defaultCharset()).trim();
                                if ("quit".equalsIgnoreCase(input)) {
                                    promise.onComplete();
                                    return promise;
                                } else {
                                    String text = "Hello " + inByteBuf.toString(Charset.defaultCharset());
                                    ByteBuf outByteBuf = Unpooled.buffer().writeBytes(text.getBytes());
                                    return connection.writeWith(Streams.just(outByteBuf));
                                }
                            }).consume();
                            return promise;
                        });
    }

    private static void echoWithCodec(TcpServer<Person, Person> transport) {
        ReactorTcpServer.create(transport)
                        .start(connection -> {
                            Promise<Void> promise = Promises.prepare();
                            connection.flatMap(inPerson -> {
                                Person outPerson = new Person()
                                        .setFirstname(inPerson.getLastname())
                                        .setLastname(inPerson.getFirstname());
                                return connection.writeWith(Streams.just(outPerson));
                            }).consume();
                            return promise;
                        });
    }

    private static class JsonCodec extends ChannelDuplexHandler {
        private final ObjectMapper mapper = new ObjectMapper();

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof ByteBuf) {
                msg = mapper.readValue(((ByteBuf) msg).toString(Charset.defaultCharset()), Person.class);
            }
            super.channelRead(ctx, msg);
        }

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof Person) {
                byte[] buff = mapper.writeValueAsBytes(msg);
                ByteBuf bb = ctx.alloc().buffer(buff.length);
                bb.writeBytes(buff);
                msg = bb;
            }
            super.write(ctx, msg, promise);
        }
    }

    private static class Person {
        private String firstname;
        private String lastname;

        public Person() {
        }

        public String getFirstname() {
            return firstname;
        }

        public Person setFirstname(String firstname) {
            this.firstname = firstname;
            return this;
        }

        public String getLastname() {
            return lastname;
        }

        public Person setLastname(String lastname) {
            this.lastname = lastname;
            return this;
        }
    }


}
