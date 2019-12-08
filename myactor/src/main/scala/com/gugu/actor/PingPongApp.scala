package com.gugu.actor

import akka.actor.{ActorRef, ActorSystem, Props}

object PingPongApp extends App {
  // actorSystem
  private val pingPongActorSystem = ActorSystem("PingPongActorSystem")
  // 通过actorSystem创建ActorRef
  // 创建FengGeActor
  private val ffActorRef: ActorRef = pingPongActorSystem.actorOf(Props[FengGeActor], "ff")
  // 创建LongGeActorRef
  private val mmActorRef: ActorRef = pingPongActorSystem.actorOf(Props(new LongGeActor(ffActorRef)),"mm")
  ffActorRef !"start"
  mmActorRef !"start"
}
