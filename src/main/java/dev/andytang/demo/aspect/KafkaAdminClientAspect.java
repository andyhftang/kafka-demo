package dev.andytang.demo.aspect;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.requests.AbstractResponse;
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

    @Around("execution(* org.apache.kafka.common.Cluster.bootstrap(..)) && args(addresses)")
    public Cluster aroundClusterBootstrap(ProceedingJoinPoint joinPoint, List<InetSocketAddress> addresses) throws Throwable {
        try {
            addresses.stream().forEach(address -> bootstrapServers.putIfAbsent(address.getHostString(), address.getPort()));
            return (Cluster) joinPoint.proceed();
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }
    }


    @Around("execution(* org.apache.kafka.clients.NetworkClient.parseResponse(..))")
    public AbstractResponse aroundNetworkClientParseResponse(ProceedingJoinPoint joinPoint) throws Throwable {
        try {
            AbstractResponse response = (AbstractResponse) joinPoint.proceed();
            if (response instanceof MetadataResponse) {
                ((MetadataResponse) response).data().brokers().stream().forEach(broker -> {
                    broker.setHost("192.168.0.203");
                    broker.setPort(bootstrapServers.get("192.168.0.203"));
                });
            }
            return response;

        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }
    }

//    @Around("within(org.apache.kafka.clients.MetadataUpdater+) && execution(* handleSuccessfulResponse(..)) && args(requestHeader,now,metadataResponse)")
//    public void aroundMetadataUpdaterHandleSuccessfulResponse(ProceedingJoinPoint joinPoint, RequestHeader requestHeader, long now, MetadataResponse metadataResponse) throws Throwable {
//        System.out.println("12345");
//        try {
//            metadataResponse.data().brokers().stream().forEach(broker -> {
//                broker.setHost("192.168.0.203");
//                broker.setPort(bootstrapServers.get("192.168.0.203"));
//            });
//            joinPoint.proceed();
//        } catch (Throwable e) {
//            e.printStackTrace();
//            throw e;
//        }
//    }

}
