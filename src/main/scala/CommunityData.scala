import com.esotericsoftware.kryo._
import com.esotericsoftware.kryo.io._

/**
 *
 * @param community
 * @param sTot
 * @param weightIn
 * @param sIn
 * @param modified
 */

class CommunityData( var community: Long, var sTot: Long, var weightIn: Long, var sIn: Long,
                     var modified: Boolean) extends Serializable with KryoSerializable
{

  def this() = this(-1L, 0L, 0L, 0L, false)

  override def toString: String = s"{community:$community,SigmaTotal:$sTot,internalWeight:$weightIn,nodeWeight:$sIn}"

  override def write(kryo: Kryo, output: Output): Unit =
  {
    kryo.writeObject(output, this.community)
    kryo.writeObject(output, this.sTot)
    kryo.writeObject(output, this.weightIn)
    kryo.writeObject(output, this.sIn)
    kryo.writeObject(output, this.modified)
  }

  override def read(kryo: Kryo, input: Input): Unit =
  {
    this.community = kryo.readObject(input, classOf[Long])
    this.sTot = kryo.readObject(input, classOf[Long])
    this.weightIn = kryo.readObject(input, classOf[Long])
    this.sIn = kryo.readObject(input, classOf[Long])
    this.modified = kryo.readObject(input, classOf[Boolean])
  }
}