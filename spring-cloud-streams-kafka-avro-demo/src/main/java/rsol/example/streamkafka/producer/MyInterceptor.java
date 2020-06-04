package rsol.example.streamkafka.producer;

import static org.hamcrest.CoreMatchers.instanceOf;

import org.springframework.integration.config.GlobalChannelInterceptor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@GlobalChannelInterceptor
@Slf4j
public class MyInterceptor implements ChannelInterceptor {

    @Override
    public Message<?> preSend(Message<?> msg, MessageChannel mc) {
//            AbstractMqMsg abstractMqMsg = (AbstractMqMsg) msg.getPayload();
//            if(StringUtils.isBlank(abstractMqMsg.getTenantId())){
//                abstractMqMsg.setTenantId(MDC.get(HeaderConstants.TENANT_ID));
//            }
//            if(StringUtils.isBlank(abstractMqMsg.getUserId())){
//                abstractMqMsg.setUserId(MDC.get(HeaderConstants.USER_ID));
//            }
    	System.out.println("-====== inside interceptor");
    	System.out.println(msg.getPayload());
    	System.out.println(msg.getHeaders().toString());
        return msg;
    }
@Override
public void postSend(Message<?> message, MessageChannel channel, boolean sent) {
	// TODO Auto-generated method stub
	ChannelInterceptor.super.postSend(message, channel, sent);
	
//	System.out.println("-====== after send interceptor " + sent);
//	System.out.println(message.getPayload());
//	System.out.println(channel.toString());
}
}