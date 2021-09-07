# team04
# 서비스 시나리오
### 기능적 요구사항
1. 고객이 메뉴를 선택하여 주문한다.
2. 고객이 결제를 한다.
3. 결제가 완료되면 주문 내역이 담당 매장으로 전달된다.
4. 매장에서 주문을 할당해 제조한다.
5. 고객이 주문을 취소할 수 있다.
6. 고객이 중간중간 주문상태를 조회한다.

### 비기능적 요구사항
1. 트랜잭션
    1. 결제가 되지않으면 주문이 진행되지 않는다 → Sync 호출
1. 장애격리
    1. 결제시스템에서 장애가 발생해도 주문취소는 24시간 받을 수 있어야한다 → Async (event-driven), Eventual Consistency
    1. 주문량이 많아 결제시스템 과중되면 잠시 주문을 받지 않고 잠시후에 하도록 유도한다 → Circuit breaker, fallback
1. 성능
    1. 고객이 주문상태를 SimpleOrderHome에서 확인 할 수 있어야 한다. → CQRS 

# Event Storming 결과

![EventStorming_V1](https://user-images.githubusercontent.com/19971917/130919916-353cbf6a-f9dc-4e96-ac37-fcbba4648fb1.png)

# 부적격 이벤트 탈락

![부적격 이벤트 탈락](https://user-images.githubusercontent.com/49510466/131070824-5da92df1-dc82-40a5-ac09-5bdc0e665db7.PNG)


# 헥사고날 아키텍처 다이어그램 도출

![헥사고날 아키텍처 다이어그램도출](https://user-images.githubusercontent.com/19971917/130926489-1e144091-ea40-40c4-91e1-ea1db26f16dd.png)


# 구현
분석/설계 단계에서 도출된 헥사고날 아키텍처에 따라, 구현한 각 서비스를 로컬에서 실행하는 방법은 아래와 같다 (각자의 포트넘버는 8081 ~ 8084, 8088 이다)
```
cd SimpleOrder
mvn spring-boot:run  

cd Payment
mvn spring-boot:run

cd SimpleOrderHome
mvn spring-boot:run 

cd Store
mvn spring-boot:run  

cd gateway
mvn spring-boot:run  
```

## DDD 의 적용
msaez.io 를 통해 구현한 Aggregate 단위로 Entity 를 선언 후, 구현을 진행하였다.

Entity Pattern 과 Repository Pattern 을 적용하기 위해 Spring Data REST 의 RestRepository 를 적용하였다.

**SimpleOrder 서비스의 SimpleOrder.java**

```java 
package team04;

import javax.persistence.*;
import org.springframework.beans.BeanUtils;

import team04.external.Payment;
import team04.external.PaymentService;

import java.util.List;

@Entity
@Table(name="SimpleOrder_table")
public class SimpleOrder {

    @Id
    @GeneratedValue(strategy=GenerationType.AUTO)
    private Long id;
    private String userId;
    private String menuId;
    private Integer qty;
    private String status;

    @PostPersist
    public void onPostPersist(){
    	Ordered ordered = new Ordered();
        BeanUtils.copyProperties(this, ordered);
        ordered.publishAfterCommit();

        //Following code causes dependency to external APIs
        // it is NOT A GOOD PRACTICE. instead, Event-Policy mapping is recommended.

        Payment payment = new Payment();
        payment.setOrderId(this.getId());
        payment.setMenuId(this.menuId);
        payment.setQty(this.getQty());
        payment.setUserId(this.getUserId());
        // mappings goes here
        SimpleOrderApplication.applicationContext.getBean(PaymentService.class)
        .pay(payment);
    }

    @PostUpdate
    public void onPostUpdate(){
        Updated updated = new Updated();
        BeanUtils.copyProperties(this, updated);
        updated.publishAfterCommit();


    }

    @PreRemove
    public void onPreRemove(){
        OrderCancelled orderCancelled = new OrderCancelled();
        BeanUtils.copyProperties(this, orderCancelled);
        orderCancelled.publishAfterCommit();


    }


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
    public String getMenuId() {
        return menuId;
    }

    public void setMenuId(String menuId) {
        this.menuId = menuId;
    }
    public Integer getQty() {
        return qty;
    }

    public void setQty(Integer qty) {
        this.qty = qty;
    }
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
    
}
```

**SimpleOrder 서비스의 PolicyHandler.java**
```java
package team04;

import team04.config.kafka.KafkaProcessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class PolicyHandler{
    @StreamListener(KafkaProcessor.INPUT)
    public void onStringEventListener(@Payload String eventString){

    }
    
    @Autowired
	SimpleOrderRepository simpleOrderRepository;

    @StreamListener(KafkaProcessor.INPUT)
    public void wheneverAssigned_(@Payload Assigned assigned){

        if(assigned.isMe()){
        	Optional<SimpleOrder> optional = simpleOrderRepository.findById(assigned.getOrderId());
        	if(optional != null && optional.isPresent())
        	{
        		SimpleOrder simpleOrder = optional.get();
        		
        		simpleOrder.setStatus("Assigned");
                // view 객체에 이벤트의 eventDirectValue를 set함
                // view 레파지토리에 save
            	simpleOrderRepository.save(simpleOrder);
        	}
            
            System.out.println("##### listener  : " + assigned.toJson());
        }
    }

}
```

- DDD 적용 후 REST API의 테스트를 통하여 정상적으로 동작하는 것을 확인할 수 있었다.  
  
- 원격 주문 (SimpleOrder 동작 후 결과)

![image](https://user-images.githubusercontent.com/49510466/131070673-73ab85a6-65d1-42d4-a6b8-c17de50e3e97.png)

# GateWay 적용
API GateWay를 통하여 마이크로 서비스들의 진입점을 통일할 수 있다.
다음과 같이 GateWay를 적용하였다.

```yaml
server:
  port: 8088

---

spring:
  profiles: default
  cloud:
    gateway:
      routes:
        - id: SimpleOrder
          uri: http://localhost:8081
          predicates:
            - Path=/simpleOrders/** 
        - id: Payment
          uri: http://localhost:8082
          predicates:
            - Path=/payments/** 
        - id: Store
          uri: http://localhost:8083
          predicates:
            - Path=/stores/** 
        - id: SimpleOrderHome
          uri: http://localhost:8084
          predicates:
            - Path= /simpleOrderHomes/**
      globalcors:
        corsConfigurations:
          '[/**]':
            allowedOrigins:
              - "*"
            allowedMethods:
              - "*"
            allowedHeaders:
              - "*"
            allowCredentials: true


---

spring:
  profiles: docker
  cloud:
    gateway:
      routes:
        - id: SimpleOrder
          uri: http://SimpleOrder:8080
          predicates:
            - Path=/simpleOrders/** 
        - id: Payment
          uri: http://Payment:8080
          predicates:
            - Path=/payments/** 
        - id: Store
          uri: http://Store:8080
          predicates:
            - Path=/stores/** 
        - id: SimpleOrderHome
          uri: http://SimpleOrderHome:8080
          predicates:
            - Path= /simpleOrderHomes/**
      globalcors:
        corsConfigurations:
          '[/**]':
            allowedOrigins:
              - "*"
            allowedMethods:
              - "*"
            allowedHeaders:
              - "*"
            allowCredentials: true

server:
  port: 8080

```

# CQRS
Materialized View 를 구현하여, 타 마이크로서비스의 데이터 원본에 접근없이(Composite 서비스나 조인SQL 등 없이) 도 내 서비스의 화면 구성과 잦은 조회가 가능하게 구현해 두었다.
본 프로젝트에서 View 역할은 SimpleOrderHomes 서비스가 수행한다.

- 주문(ordered) 실행 후 SimpleOrderHomes 화면

![image](https://user-images.githubusercontent.com/49510466/131071310-908d5804-7c43-4a19-8ac7-ff742546665d.png)

- 주문(OrderCancelled) 취소 후 SimpleOrderHomes 화면

![image](https://user-images.githubusercontent.com/49510466/131072048-247bfdcc-1f34-4bf1-8407-cb20bf93ade3.png)

위와 같이 주문을 하게되면 SimpleOrder -> Payment -> Store -> SimpleOrderHome로 주문이 처리되어 Assigned 되고

주문 취소가 되면 Status가 refunded로 Update 되는 것을 볼 수 있다.

또한 Correlation을 key를 활용하여 orderId를 Key값으로 하고 원하는 주문하고 서비스간의 공유가 이루어지도록 하였다.

위의 결과로 서로 다른 마이크로서비스 간에 트랜잭션이 묶여 있음을 알 수 있다.

# 폴리글랏

Store 서비스의 DB와 SimpleOrder의 DB를 다른 DB를 사용하여 폴리글랏을 만족시키고 있다.

**Store의 pom.xml DB 설정 코드**

![image](https://user-images.githubusercontent.com/49510466/131072664-7e52d462-0f39-41d0-859f-44654acacd29.png)

**SimpleOrder의 pom.xml DB 설정 코드**

![image](https://user-images.githubusercontent.com/49510466/131072537-bdc1a2f8-c431-4286-88b3-1e7c64cb0bcb.png)

# 동기식 호출 과 Fallback 처리

분석단계에서의 조건 중 하나로 주문(SimpleOrder)->결제(pay) 간의 호출은 동기식 일관성을 유지하는 트랜잭션으로 처리하기로 하였다. 
호출 프로토콜은 Rest Repository에 의해 노출되어있는 REST 서비스를 FeignClient를 이용하여 호출하도록 한다.

**SimpleOrder 서비스 내 external.PaymentService**
```java
package team04.external;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.Date;

@FeignClient(name="Payment", url="${api.url.Payment}")
public interface PaymentService {

    @RequestMapping(method= RequestMethod.POST, path="/payments")
    public void pay(@RequestBody Payment payment);

}
```

**동작 확인**
- 잠시 Payment 서비스 중지시킴

![image](https://user-images.githubusercontent.com/49510466/131073007-a286c684-c4a7-4fc1-a941-9d05d27d5b75.png)

- 주문 요청시 에러 발생

![image](https://user-images.githubusercontent.com/49510466/131072921-dba5f43f-9b76-4783-87c1-59710509ff0b.png)

- Payment 서비스 재기동 후 정상동작 확인

![image](https://user-images.githubusercontent.com/49510466/131074016-3e56146e-67bc-4f96-9589-9397aa61b7f8.png)
![image](https://user-images.githubusercontent.com/49510466/131073927-927d81f7-461b-465e-b397-05ba1cb418ce.png)

# 운영

# Deploy / Pipeline

- git에서 소스 가져오기
```
git clone https://github.com/hyeonwoos/simpleOrder.git
```
- Build 하기
```
cd /simpleOrder
cd gateway
mvn package

cd ..
cd SimpleOrder
mvn package

cd ..
cd Payment
mvn package

cd ..
cd Store
mvn package

cd ..
cd SimpleOrderHome
mvn package
```

- Docker Image Push/deploy/서비스생성
```
cd gateway
docker build -t user21.azurecr.io/gateway:v1 .
docker images
docker push user21.azurecr.io/gateway:v1

cd kubernetes
kubectl create ns tutorial
kubectl create -f service.yaml
kubectl create -f deployment.yml

cd ..
cd Payment
docker build -t user21.azurecr.io/payment:v1 .
docker images
docker push user21.azurecr.io/payment:v1

kubectl create -f service.yaml
kubectl get all -n tutorial (payment IP 10.0.185.189 확인)
kubectl create configmap apiurl --from-literal=url=http://10.0.185.189:8080 -n tutorial
kubectl create -f deployment.yml


cd ..
cd Store
docker build -t user21.azurecr.io/store:v1 .
docker images
docker push user21.azurecr.io/store:v1

kubectl create -f service.yaml
kubectl create -f deployment.yml

cd ..
cd SimpleOrderHome

docker build -t user21.azurecr.io/simpleorderhome:v1 .
docker images
docker push user21.azurecr.io/simpleorderhome:v1

kubectl create -f service.yaml
kubectl create -f deployment.yml

```

- yml파일 이용한 deploy
```
cd ..
cd  SimpleOrder
docker build -t user21.azurecr.io/simpleorder:v1 .
docker push user21.azurecr.io/simpleorder:v1
```
![image](https://user-images.githubusercontent.com/49510466/131075585-a8a4d4df-3fe8-4c5e-81fb-46e7e91686b6.png)

```
kubectl expose deploy store --type=ClusterIP --port=8080 -n tutorial
```

- team04/SimpleOrder/kubernetes/deployment.yml 파일 
```yml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: simpleorder
  namespace: tutorial
  labels:
    app: simpleorder
spec:
  replicas: 1
  selector:
    matchLabels:
      app: simpleorder
  template:
    metadata:
      labels:
        app: simpleorder
    spec:
      containers:
        - name: simpleorder
          image: user21.azurecr.io/simpleorder:v1
          ports:
            - containerPort: 8080
          env:
            - name: configurl
              valueFrom:
                configMapKeyRef:
                  name: apiurl
                  key: url

```	  
- deploy 완료

![image](https://user-images.githubusercontent.com/49510466/131075892-47da833e-527b-4ae5-8103-d9043a5f9100.png)

# ConfigMap 
- 시스템별로 변경 가능성이 있는 설정들을 ConfigMap을 사용하여 관리

- application.yml 파일에 ${configurl} 설정

```yaml
      feign:
        hystrix:
          enabled: true
      hystrix:
        command:
          default:
            execution.isolation.thread.timeoutInMilliseconds: 610
      api:
        url:
          Payment: ${configurl}

```

- ConfigMap 사용(/SimpleOrder/src/main/java/team04/external/PaymentService.java) 

```java

      @FeignClient(name="Payment", url="${api.url.Payment}")
      public interface PaymentService {
      
	      @RequestMapping(method= RequestMethod.POST, path="/payments")
              public void pay(@RequestBody Payment payment);
	      
      }
```

- Deployment.yml 에 ConfigMap 적용

![image](https://user-images.githubusercontent.com/49510466/131075991-013980f8-60c7-4711-8e47-fd9afc0ef693.png)

- ConfigMap 생성

```
kubectl create configmap apiurl --from-literal=url=http://10.0.185.189:8080 -n tutorial 
```


# 오토스케일 아웃

- 서킷 브레이커는 시스템을 안정되게 운영할 수 있게 해줬지만, 사용자의 요청이 급증하는 경우, 오토스케일 아웃이 필요하다.

>- 단, 부하가 제대로 걸리기 위해서, recipe 서비스의 리소스를 줄여서 재배포한다.(team04/Store/kubernetes/deployment.yml 수정)

```yaml
          resources:
            limits:
              cpu: 500m
            requests:
              cpu: 200m
```

- 다시 expose 해준다.
```
kubectl expose deploy store --type=ClusterIP --port=8080 -n tutorial
```
- recipe 시스템에 replica를 자동으로 늘려줄 수 있도록 HPA를 설정한다. 설정은 CPU 사용량이 15%를 넘어서면 replica를 10개까지 늘려준다.
```
kubectl autoscale deploy store --min=1 --max=10 --cpu-percent=15 -n tutorial
```
- siege를 활용해서 워크로드를 2분간 걸어준다. (Cloud 내 siege pod에서 부하줄 것)
```
kubectl exec -it siege -c siege -- /bin/bash 
root@siege:/# siege -c100 -t60S -r10 -v --content-type "application/json" 'http://20.200.207.107:8080/stores POST {"orderId": 111, "userId": "user10", "menuId": "menu10", "qty":10}'

```
![image](https://user-images.githubusercontent.com/49510466/131079991-3cee4245-9d39-4e50-83ff-d49cee4aad34.png)
- 오토스케일 모니터링을 걸어 스케일 아웃이 자동으로 진행됨을 확인한다.
```
kubectl get all -n tutorial
```
![오토스케일아웃_결과](https://user-images.githubusercontent.com/49510466/131082237-dfc12c8f-d3fb-4da8-a813-81a0f4058e0f.png)

# 서킷 브레이킹

- 서킷 브레이킹 프레임워크의 선택 : Spring FeignClient + Hystrix 옵션을 사용하여 구현함
- Hystrix를 설정 : 요청처리 쓰레드에서 처리시간이 610 밀리가 넘어서기 시작하여 어느정도
  유지되면 CB 회로가 닫히도록(요청을 빠르게 실패처리, 차단) 설정

- 동기 호출 주체인 SimpleOrder에서 Hystrix 설정 
- SimpleOrder/src/main/resources/application.yml 파일
```yaml
feign:
  hystrix:
    enabled: true
hystrix:
  command:
    default:
      execution.isolation.thread.timeoutInMilliseconds: 610
```

- 부하에 대한 지연시간 발생코드
- team04/SimpleOrder/src/main/java/team04/external/PaymentService.java
``` java
    @PostPersist
    public void onPostPersist(){
        Payed payed = new Payed();
        BeanUtils.copyProperties(this, payed);
        payed.publishAfterCommit();
        
        try {
                Thread.currentThread().sleep((long) (400 + Math.random() * 220));
        } catch (InterruptedException e) {
                e.printStackTrace();
        }
    }
```

- 부하 테스터 siege툴을 통한 서킷 브레이커 동작확인 :
  
  동시 사용자 100명, 60초 동안 실시 
```
siege -c100 -t60S -r10 -v --content-type "application/json" '20.200.229.147:8080/simpleOrders POST {"userId": "user10", "menuId": "menu10", "qty":10}'
```
- 부하 발생하여 CB가 발동하여 요청 실패처리하였고, 밀린 부하가 다시 처리되면서 SimpleOrders를 받기 시작

![서킷브레이킹(증빙10)](https://user-images.githubusercontent.com/88122579/131077639-a684ec4d-4705-4816-821e-ff09cee8855b.png)

# 무정지 배포

- 무정지 배포가 되지 않는 readiness 옵션을 제거 설정
team04/Store/kubernetes/deployment_n_readiness.yml
```yml
    spec:
      containers:
        - name: store
          image: user12.azurecr.io/store:v1
          ports:
            - containerPort: 8080
#          readinessProbe:
#            httpGet:
#              path: '/actuator/health'
#              port: 8080
#            initialDelaySeconds: 10
#            timeoutSeconds: 2
#            periodSeconds: 5
#            failureThreshold: 10
          livenessProbe:
            httpGet:
              path: '/actuator/health'
              port: 8080
            initialDelaySeconds: 120
            timeoutSeconds: 2
            periodSeconds: 5
            failureThreshold: 5
```
- 무정지 배포가 되지 않아 Siege 결과 Availability가 100%가 되지 않음

![image](https://user-images.githubusercontent.com/49510466/131080826-3a8dd061-2059-4f59-99b8-13e7d794e93e.png)
![image](https://user-images.githubusercontent.com/49510466/131081804-1e8916a1-383a-41bb-89dc-4528c4066a66.png)

- 무정지 배포를 위한 readiness 옵션 설정
team04/Store/kubernetes/deployment.yml
```yml
    spec:
      containers:
        - name: store
          image: user12.azurecr.io/store:v1
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet:
              path: '/actuator/health'
              port: 8080
            initialDelaySeconds: 10
            timeoutSeconds: 2
            periodSeconds: 5
            failureThreshold: 10
          livenessProbe:
            httpGet:
              path: '/actuator/health'
              port: 8080
            initialDelaySeconds: 120
            timeoutSeconds: 2
            periodSeconds: 5
            failureThreshold: 5
```

- 무정지 배포를 위한 readiness 옵션 설정 후 적용 시 Siege 결과 Availability가 100% 확인

![image](https://user-images.githubusercontent.com/49510466/131081370-4ac1495e-7322-4a6b-b25a-c5f9949f228f.png)
![image](https://user-images.githubusercontent.com/49510466/131081754-16cfa937-6f9e-4634-9488-5ba21271f47e.png)

# Self-healing (Liveness Probe)

- Self-healing 확인을 위한 Liveness Probe 옵션 변경
![image](https://user-images.githubusercontent.com/49510466/131082754-fbc0327b-18e7-498f-ae5c-030ab1112b09.png)

team04/Store/kubernetes/deployment_live.yml
```yml
          readinessProbe:
            httpGet:
              path: '/actuator/health'
              port: 8080
            initialDelaySeconds: 10
            timeoutSeconds: 2
            periodSeconds: 5
            failureThreshold: 10
          livenessProbe:
            httpGet:
              path: '/actuator/health'
              port: 8081
            initialDelaySeconds: 5
            periodSeconds: 5
```

- Store pod에 Liveness Probe 옵션 적용 확인

![image](https://user-images.githubusercontent.com/49510466/131083494-d559d9fb-6f68-4882-aefc-f4e5258dec4d.png)

- Store pod에서 적용 시 retry발생 확인

![image](https://user-images.githubusercontent.com/49510466/131084109-83441877-f9b6-44da-a469-d2978e1491f3.png)

