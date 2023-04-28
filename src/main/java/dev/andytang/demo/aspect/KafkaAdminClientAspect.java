package dev.andytang.demo.aspect;

import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Aspect
public class KafkaAdminClientAspect {

    private Map<String, Integer> bootstrapServers = new ConcurrentHashMap<>();

    @Around("execution(* org.apache.kafka.clients.Metadata.bootstrap(..)) && args(addresses)")
    public void aroundMetadataBootstrap(ProceedingJoinPoint joinPoint, List<InetSocketAddress> addresses) throws Throwable {
        try {
            addresses.stream().forEach(address -> bootstrapServers.putIfAbsent(address.getHostString(), address.getPort()));
            joinPoint.proceed();
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Around("within(org.apache.kafka.clients.MetadataUpdater+) && execution(* handleSuccessfulResponse(..)) && args(requestHeader,now,metadataResponse)")
    public void aroundMetadataUpdaterHandleSuccessfulResponse(ProceedingJoinPoint joinPoint, RequestHeader requestHeader, long now, MetadataResponse metadataResponse) throws Throwable {
        System.out.println("12345");
        try {
            metadataResponse.data().brokers().stream().forEach(broker -> broker.setHost("192.168.0.203"));
            Object[] args = joinPoint.getArgs();
            args[2] = new MetadataResponse(metadataResponse.data(), (short) (metadataResponse.hasReliableLeaderEpochs() ? 9 : 0));
            joinPoint.proceed(args);
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }
    }

}
