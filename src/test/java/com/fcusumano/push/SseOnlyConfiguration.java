package com.fcusumano.push;

import com.fcusumano.push.annotation.EnablePushMessaging;
import org.springframework.context.annotation.Configuration;

//Applicazione solo SSE senza riferimenti al messaging: caricabile anche da un class loader che non lo vede
@Configuration(proxyBeanMethods = false)
@EnablePushMessaging
class SseOnlyConfiguration {}
