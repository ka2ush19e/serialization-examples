package jp.gr.java_conf.ka2ush19e.serializations.avro

import java.net.InetSocketAddress

import org.apache.avro.ipc.NettyServer
import org.apache.avro.ipc.specific.SpecificResponder

import jp.gr.java_conf.ka2ush19e.serializations.avro.protocol.{ItemPurchase, Shopping}

object ShoppingNettyServer {

  class ShoppingImpl extends Shopping {
    override def purchase(item_purchase: ItemPurchase): String = {
      println("Receive message")
      println(s"  User:      ${item_purchase.getUser}")
      println(s"  Item:      ${item_purchase.getItem}")
      println(s"  Quantity:  ${item_purchase.getQuantity}")
      println(s"  UnitPrice: ${item_purchase.getUnitPrice}")
      println()
      "OK"
    }
  }

  def main(args: Array[String]) {
    println("Starting server")
    val server = new NettyServer(
      new SpecificResponder(classOf[Shopping], new ShoppingImpl()),
      new InetSocketAddress(65111)
    )
    println("Started")

    println("Waiting request")
    server.join()
  }
}
