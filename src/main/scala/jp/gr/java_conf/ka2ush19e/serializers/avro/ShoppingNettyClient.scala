package jp.gr.java_conf.ka2ush19e.serializers.avro

import java.net.InetSocketAddress

import org.apache.avro.ipc.NettyTransceiver
import org.apache.avro.ipc.specific.SpecificRequestor
import org.apache.avro.util.Utf8

import jp.gr.java_conf.ka2ush19e.serializers.avro.protocol.{ItemPurchase, Shopping}

object ShoppingNettyClient {
  def main(args: Array[String]) {
    val client = new NettyTransceiver(new InetSocketAddress("localhost", 65111))
    val proxy = SpecificRequestor.getClient(classOf[Shopping], client)

    val itemPurchase = new ItemPurchase()
    itemPurchase.setUser(new Utf8("user1"))
    itemPurchase.setItem(new Utf8("item1"))
    itemPurchase.setQuantity(1)
    itemPurchase.setUnitPrice(198)

    println(proxy.purchase(itemPurchase))

    client.close()
  }
}
