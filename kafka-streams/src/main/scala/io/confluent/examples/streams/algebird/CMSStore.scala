/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams.algebird

import com.twitter.algebird.{CMSHasher, TopCMS, TopPctCMS}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.processor.{ProcessorContext, StateStore}
import org.apache.kafka.streams.state.StateSerdes

/**
  * An in-memory store that leverages the Count-Min Sketch implementation of
  * [[https://github.com/twitter/algebird Twitter Algebird]].
  *
  * This store allows you to probabilistically count items of type T with a
  * [[https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch Count-Min Sketch]] data structure.
  * Here, the counts returned by the store will be approximate counts, i.e. estimations, because a
  * Count-Min Sketch trades slightly inaccurate counts for greatly reduced space utilization
  * (however, the estimation error is mathematically proven to be bounded).
  * With probability at least `1 - delta`, this estimate is within `eps * N` of the true frequency
  * (i.e., `true frequency <= estimate <= true frequency + eps * N`), where `N` is the total number
  * of items counted ("seen" in the input) so far (cf. [[CMSStore#totalCount]]).
  *
  * A traditional Count-Min Sketch is a fixed-size data structure that is essentially an array of
  * counters of a particular width (derived from the parameter `eps`) and depth (derived from the
  * parameter `delta`).  The CMS variant used in this store, [[TopPctCMS]], additionally tracks the
  * so-called "heavy hitters" among the counted items (i.e. the items with the largest counts) based
  * on a percentage threshold; the size of heavy hitters is still bounded, however, hence the total
  * size of the [[TopPctCMS]] data structure is still fixed.
  *
  * =Limitations and future work=
  *
  * This store is not (yet) logging its changes to Kafka and is thus not fault-tolerant.  To
  * support changelogging see [[org.apache.kafka.streams.state.internals.StoreChangeLogger]]
  * (we should not use keys of type `Array[Byte]`, cf. caveats in Javadocs) and
  * [[org.apache.kafka.streams.state.internals.InMemoryKeyValueLoggedStore]].
  *
  * =Usage=
  *
  * Note: Twitter Algebird is best used with Scala, so all the examples below are in Scala, too.
  *
  * In a Kafka Streams application, you'd typically create this store as such:
  *
  * {{{
  * val builder: KStreamBuilder = new KStreamBuilder()
  *
  * // In this example, we create a store for type [[String]].
  * builder.addStateStore(new CMSStoreSupplier[String]("my-cms-store-name", Serdes.String()))
  * }}}
  *
  * Then you'd use the store within a [[org.apache.kafka.streams.processor.Processor]] or a
  * [[org.apache.kafka.streams.kstream.Transformer]] similar to:
  *
  * {{{
  * class ProbabilisticCounter extends Transformer[Array[Byte], String, KeyValue[String, Long]] {
  *
  *   private var cmsState: CMSStore[String] = _
  *   private var processorContext: ProcessorContext = _
  *
  *   override def init(processorContext: ProcessorContext): Unit = {
  *     this.processorContext = processorContext
  *     cmsState = this.processorContext.getStateStore("my-cms-store-name").asInstanceOf[CMSStore[String]]
  *   }
  *
  *   override def transform(key: Array[Byte], value: String): KeyValue[String, Long] = {
  *     // Count the record value, think: "+ 1"
  *     cmsState.put(value)
  *
  *     // Emit the latest count estimate for the record value
  *     KeyValue.pair[String, Long](value, cmsState.get(value))
  *   }
  *
  *   override def punctuate(l: Long): KeyValue[String, Long] = null
  *
  *   override def close(): Unit = {}
  * }
  * }}}
  *
  * @param name            The name of this store instance
  * @param serde           The serde for the type T
  * @param delta           CMS parameter: A bound on the probability that a query estimate does not
  *                        lie within some small interval (an interval that depends on `eps`) around
  *                        the truth.
  *                        See [[TopPctCMS]] and [[com.twitter.algebird.CMSMonoid]].
  * @param eps             CMS parameter: One-sided error bound on the error of each point query,
  *                        i.e. frequency estimate.
  *                        See [[TopPctCMS]] and [[com.twitter.algebird.CMSMonoid]].
  * @param seed            CMS parameter: A seed to initialize the random number generator used to
  *                        create the pairwise independent hash functions.  Typically you do not
  *                        need to change this.
  *                        See [[TopPctCMS]] and [[com.twitter.algebird.CMSMonoid]].
  * @param heavyHittersPct CMS parameter: A threshold for finding heavy hitters, i.e., items that
  *                        appear at least (heavyHittersPct * totalCount) times in the stream.
  *                        Every item that appears at least `(heavyHittersPct * totalCount)` times
  *                        is included, and with probability `p >= 1 - delta`, no item whose count
  *                        is less than `(heavyHittersPct - eps) * totalCount` is included.
  *                        This also means that this parameter is an upper bound on the number of
  *                        heavy hitters that will be tracked: the set of heavy hitters contains at
  *                        most `1 / heavyHittersPct` elements.  For example, if
  *                        `heavyHittersPct=0.01` (or 0.25), then at most `1 / 0.01 = 100` items
  *                        or `1 / 0.25 = 4` items) will be tracked/returned as heavy hitters.
  *                        This parameter can thus control the memory footprint required for
  *                        tracking heavy hitters.
  *                        See [[TopPctCMS]] and [[com.twitter.algebird.TopPctCMSMonoid]].
  * @tparam T The type used to identify the items to be counted with the CMS.  For example, if
  *           you want to count the occurrence of user names, you could use count user names
  *           directly with `T=String`;  alternatively, you could map each username to a unique
  *           numeric ID expressed as a `Long`, and then count the occurrences of those `Long`s with
  *           a CMS of type `T=Long`.  Note that such a mapping between the items of your problem
  *           domain and their identifiers used for counting via CMS should be bijective.
  *           We require a [[CMSHasher]] context bound for `K`, see [[CMSHasher]] for available
  *           implicits that can be imported.
  *           See [[com.twitter.algebird.CMSMonoid]] for further information.
  */
class CMSStore[T: CMSHasher](val name: String,
                             val serde: Serde[T],
                             val delta: Double = 1E-10,
                             val eps: Double = 0.001,
                             val seed: Int = 1,
                             val heavyHittersPct: Double = 0.01)
    extends StateStore {

  private val cmsMonoid = TopPctCMS.monoid[T](eps, delta, seed, heavyHittersPct)

  private var cms: TopCMS[T] = _

  @volatile private var open: Boolean = false

  private var serdes: StateSerdes[T, Long] = _

  def init(context: ProcessorContext, root: StateStore) {
    this.serdes = new StateSerdes[T, Long](
      name,
      if (serde == null) context.keySerde.asInstanceOf[Serde[T]] else serde,
      Serdes.Long().asInstanceOf[Serde[Long]])

    cms = cmsMonoid.zero
    context.register(root, true, (key, value) => {
      // TODO: Implement the state store restoration function (which would restore the `cms` instance)
    })

    open = true
  }

  /**
    * Returns the estimated count of the item.
    *
    * @param item item to be counted
    * @return estimated count
    */
  def get(item: T): Long = cms.frequency(item).estimate

  /**
    * Counts the item.
    *
    * @param item item to be counted
    */
  def put(item: T): Unit = cms = cms + item

  /**
    * The top items counted so far, with the percentage-based cut-off being defined by the CMS
    * parameter `heavyHittersPct`.
    *
    * @return the top items counted so far
    */
  def heavyHitters: Set[T] = cms.heavyHitters

  /**
    * Returns the total number of items counted ("seen" in the input) so far.
    *
    * This number is not the same as the total number of <em>unique</em> items counted so far, i.e.
    * it is not the cardinality of the set of items.
    *
    * Example: After having counted the input "foo", "bar", "foo", the return value would be 3.
    *
    * @return number of count operations so far
    */
  def totalCount: Long = cms.totalCount

  override val persistent: Boolean = false

  override def isOpen: Boolean = open

  override def flush() {
    // do nothing because this store is in-memory
  }

  override def close() {
    open = false
  }

}