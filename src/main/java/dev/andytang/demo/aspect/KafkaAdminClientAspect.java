package dev.andytang.demo.aspect;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.requests.MetadataResponse;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

import java.util.List;
import java.util.stream.Collectors;

@Aspect
public class KafkaAdminClientAspect {

//    @Before("execution(* org.apache.kafka.clients.admin.internals.AdminMetadataManager.update(..)) && args(cluster,..)")
//    public void before(ProceedingJoinPoint joinPoint, Cluster cluster) throws IllegalAccessException {
//        List<Node> oldNodes = ((AdminMetadataManager) joinPoint.getTarget()).updater()
//                                                                            .fetchNodes();
//        if (!oldNodes.isEmpty() && oldNodes.get(0).id() == -1) {
//            FieldUtils.writeField(cluster, "nodes", cluster.nodes()
//                                                           .stream()
//                                                           .map(node -> new Node(node.id(), oldNodes.get(0).host(), node.port(), node.rack()))
//                                                           .collect(Collectors.toList()), true);
//        }
//    }
//
//    @After("execution(* org.apache.kafka.clients.admin.internals.AdminMetadataManager.update(..))")
//    public void after() {
//        System.out.println("after update");
//    }
//
//    @AfterReturning(pointcut = "pointcut()", returning = "returnObject")
//    public void afterReturning(JoinPoint joinPoint, Object returnObject) {
//        System.out.println("afterReturning");
//    }
//
//    @AfterThrowing("pointcut()")
//    public void afterThrowing() {
//        System.out.println("afterThrowing afterThrowing  rollback");
//    }

    @Around("execution(* org.apache.kafka.clients.admin.internals.AdminMetadataManager.update(..)) && args(cluster,..)")
    public Object aroundAdminMetadataManagerUpdate(ProceedingJoinPoint joinPoint, Cluster cluster) throws Throwable {
        try {
            List<Node> oldNodes = ((AdminMetadataManager) joinPoint.getTarget()).updater().fetchNodes();
            if (!oldNodes.isEmpty() && oldNodes.get(0).id() == -1) {
                FieldUtils.writeField(cluster,
                        "nodes",
                        cluster.nodes()
                               .stream()
                               .map(node -> {
                                   return new Node(node.id(), oldNodes.get(0).host(), node.port(), node.rack());
                               })
                               .collect(Collectors.toList()),
                        true);
            }
            return joinPoint.proceed();
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Around("execution(* org.apache.kafka.clients.Metadata.update(..)) && args(requestVersion, response,..)")
    public Object around(ProceedingJoinPoint joinPoint, int requestVersion, MetadataResponse response) throws Throwable {
        try {
            System.out.println("around 1");
            List<Node> oldNodes = ((Metadata) joinPoint.getTarget()).fetch().nodes();
            if (!oldNodes.isEmpty() && oldNodes.get(0).id() == -1) {
                MetadataResponseData.MetadataResponseBrokerCollection brokers = new MetadataResponseData.MetadataResponseBrokerCollection(response.data().brokers().size());
                response.data().brokers().stream().forEach(broker -> broker.setHost(oldNodes.get(0).host()));
            }
            return joinPoint.proceed();
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }
    }

}
