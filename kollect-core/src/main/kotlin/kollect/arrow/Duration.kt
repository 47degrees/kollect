package kollect.arrow

import arrow.core.Some
import arrow.core.toOption
import kollect.arrow.Duration.Companion.Infinite
import java.lang.Math
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.*
import java.lang.Double as JDouble
import java.lang.Long as JLong
import java.lang.Long.numberOfLeadingZeros as leading

/**
 * <h2>Utility for working with java.util.concurrent.TimeUnit durations.</h2>
 *
 * '''''This class is not meant as a general purpose representation of time, it is
 * optimized for the needs of `scala.concurrent`.'''''
 *
 * <h2>Basic Usage</h2>
 *
 * <p/>
 * Examples:
 * {{{
 * import scala.concurrent.duration._
 *
 * val duration = Duration(100, MILLISECONDS)
 * val duration = Duration(100, "millis")
 *
 * duration.toNanos
 * duration < 1.second
 * duration <= Duration.Inf
 * }}}
 *
 * '''''Invoking inexpressible conversions (like calling `toSeconds` on an infinite duration) will throw an IllegalArgumentException.'''''
 *
 * <p/>
 * Implicits are also provided for Int, Long and Double. Example usage:
 * {{{
 * import scala.concurrent.duration._
 *
 * val duration = 100 millis
 * }}}
 *
 * '''''The DSL provided by the implicit conversions always allows construction of finite durations, even for infinite Double inputs; use Duration.Inf instead.'''''
 *
 * Extractors, parsing and arithmetic are also included:
 * {{{
 * val d = Duration("1.2 µs")
 * val Duration(length, unit) = 5 millis
 * val d2 = d * 2.5
 * val d3 = d2 + 1.millisecond
 * }}}
 *
 * <h2>Handling of Time Units</h2>
 *
 * Calculations performed on finite durations always retain the more precise unit of either operand, no matter
 * whether a coarser unit would be able to exactly express the same duration. This means that Duration can be
 * used as a lossless container for a (length, unit) pair if it is constructed using the corresponding methods
 * and no arithmetic is performed on it; adding/subtracting durations should in that case be done with care.
 *
 * <h2>Correspondence to Double Semantics</h2>
 *
 * The semantics of arithmetic operations on Duration are two-fold:
 *
 *  - exact addition/subtraction with nanosecond resolution for finite durations, independent of the summands' magnitude
 *  - isomorphic to `java.lang.Double` when it comes to infinite or undefined values
 *
 * The conversion between Duration and Double is done using [[Duration.toUnit]] (with unit NANOSECONDS)
 * and [[Duration$.fromNanos(nanos:Double)* Duration.fromNanos(Double)]]
 *
 * <h2>Ordering</h2>
 *
 * The default ordering is consistent with the ordering of Double numbers, which means that Undefined is
 * considered greater than all other durations, including [[Duration.Inf]].
 *
 * @define exc @throws IllegalArgumentException when invoked on a non-finite duration
 *
 * @define ovf @throws IllegalArgumentException in case of a finite overflow: the range of a finite duration is +-(2^63-1)ns, and no conversion to infinite durations takes place.
 */
sealed abstract class Duration : Comparable<Duration> {

    companion object {
        /**
         * Construct a Duration from the given length and unit. Observe that nanosecond precision may be lost if
         *
         *  - the unit is NANOSECONDS
         *  - and the length has an absolute value greater than 2^53
         *
         * Infinite inputs (and NaN) are converted into [[Duration.Inf]], [[Duration.MinusInf]] and [[Duration.Undefined]], respectively.
         *
         * @throws IllegalArgumentException if the length was finite but the resulting duration cannot be expressed as a [[FiniteDuration]]
         */
        inline operator fun invoke(length: Double, unit: TimeUnit): Duration = fromNanos(unit.toNanos(1) * length)

        /**
         * Construct a finite duration from the given length and time unit. The unit given is retained
         * throughout calculations as long as possible, so that it can be retrieved later.
         */
        inline operator fun invoke(length: Long, unit: TimeUnit): FiniteDuration = FiniteDuration(length, unit)

        /**
         * Construct a finite duration from the given length and time unit, where the latter is
         * looked up in a list of string representation. Valid choices are:
         *
         * `d, day, h, hour, min, minute, s, sec, second, ms, milli, millisecond, µs, micro, microsecond, ns, nano, nanosecond`
         * and their pluralized forms (for every but the first mentioned form of each unit, i.e. no "ds", but "days").
         */
        inline operator fun invoke(length: Long, unit: String): FiniteDuration = FiniteDuration(length, Duration.timeUnit[unit]!!)

        // Double stores 52 bits mantissa, but there is an implied '1' in front, making the limit 2^53
        val maxPreciseDouble: Double = 9007199254740992.0

        /**
         * Parse String into Duration.  Format is `"<length><unit>"`, where
         * whitespace is allowed before, between and after the parts. Infinities are
         * designated by `"Inf"`, `"PlusInf"`, `"+Inf"` and `"-Inf"` or `"MinusInf"`.
         *
         * @throws NumberFormatException if format is not parsable
         */
        inline operator fun invoke(s: String): Duration {
            val s1: String = s.filterNot { it == ' ' }
            return when (s1) {
                "Inf", "PlusInf", "+Inf" -> Infinite.Inf()
                "MinusInf", "-Inf" -> Infinite.MinusInf()
                else -> {
                    val unitName = s1.reversed().takeWhile { it.isLetter() }.reversed()
                    val optionUnit = timeUnit[unitName].toOption()
                    when (optionUnit) {
                        is Some<TimeUnit> -> {
                            val valueStr = s1.dropLast(unitName.length)
                            val valueD = JDouble.parseDouble(valueStr)
                            if (valueD >= -maxPreciseDouble && valueD <= maxPreciseDouble) Duration(valueD, optionUnit.t)
                            else Duration(JLong.parseLong(valueStr), optionUnit.t)
                        }
                        else -> throw NumberFormatException("format error $s")
                    }
                }
            }
        }

        // "ms milli millisecond" -> List("ms", "milli", "millis", "millisecond", "milliseconds")
        private fun words(s: String) = (s.trim().split("\\s+").toList())

        private fun expandLabels(labels: String): List<String> = listOf(words(labels).first()) + words(labels).drop(1).flatMap { s -> listOf(s, s + "s") }

        private val timeUnitLabels = listOf(
                TimeUnit.DAYS to "d day",
                HOURS to "h hour",
                MINUTES to "min minute",
                SECONDS to "s sec second",
                MILLISECONDS to "ms milli millisecond",
                MICROSECONDS to "µs micro microsecond",
                NANOSECONDS to "ns nano nanosecond"
        )

        // TimeUnit => standard label
        val timeUnitName: Map<TimeUnit, String> = timeUnitLabels.toMap().mapValues { s -> words(s.value).last() }.toMap()

        // Label => TimeUnit
        val timeUnit: Map<String, TimeUnit> = timeUnitLabels.flatMap {
            val unit = it.first
            val names = it.second
            expandLabels(names).map { Pair(it, unit) }
        }.toMap()

        /**
         * Construct a possibly infinite or undefined Duration from the given number of nanoseconds.
         *
         *  - `Double.PositiveInfinity` is mapped to [[Duration.Inf]]
         *  - `Double.NegativeInfinity` is mapped to [[Duration.MinusInf]]
         *  - `Double.NaN` is mapped to [[Duration.Undefined]]
         *  - `-0d` is mapped to [[Duration.Zero]] (exactly like `0d`)
         *
         * The semantics of the resulting Duration objects matches the semantics of their Double
         * counterparts with respect to arithmetic operations.
         *
         * @throws IllegalArgumentException if the length was finite but the resulting duration cannot be expressed as a [[FiniteDuration]]
         */
        fun fromNanos(nanos: Double): Duration = if (nanos.isInfinite())
            if (nanos > 0) Infinite.Inf() else Infinite.MinusInf()
        else if (JDouble.isNaN(nanos))
            Infinite.Undefined()
        else if (nanos > Long.MAX_VALUE || nanos < Long.MIN_VALUE)
            throw IllegalArgumentException("trying to construct too large duration with " + nanos + "ns")
        else
            fromNanos(Math.round(nanos))

        private val µs_per_ns = 1000L
        private val ms_per_ns = µs_per_ns * 1000
        private val s_per_ns = ms_per_ns * 1000
        private val min_per_ns = s_per_ns * 60
        private val h_per_ns = min_per_ns * 60
        private val d_per_ns = h_per_ns * 24

        /**
         * Construct a finite duration from the given number of nanoseconds. The
         * result will have the coarsest possible time unit which can exactly express
         * this duration.
         *
         * @throws IllegalArgumentException for `Long.MinValue` since that would lead to inconsistent behavior afterwards (cannot be negated)
         */
        fun fromNanos(nanos: Long): FiniteDuration =
                if (nanos % d_per_ns == 0L) Duration(nanos / d_per_ns, TimeUnit.DAYS)
                else if (nanos % h_per_ns == 0L) Duration(nanos / h_per_ns, HOURS)
                else if (nanos % min_per_ns == 0L) Duration(nanos / min_per_ns, MINUTES)
                else if (nanos % s_per_ns == 0L) Duration(nanos / s_per_ns, SECONDS)
                else if (nanos % ms_per_ns == 0L) Duration(nanos / ms_per_ns, MILLISECONDS)
                else if (nanos % µs_per_ns == 0L) Duration(nanos / µs_per_ns, MICROSECONDS)
                else Duration(nanos, NANOSECONDS)

        /**
         * Preconstructed value of `0.days`.
         */
        // unit as coarse as possible to keep (_ + Zero) sane unit-wise
        val Zero: FiniteDuration = FiniteDuration(0, TimeUnit.DAYS)

        sealed class Infinite : Duration() {
            override operator fun plus(other: Duration): Duration = when {
                other is Undefined -> Undefined()
                other is Infinite && other != this -> Undefined()
                else -> this
            }

            override operator fun minus(other: Duration): Duration = when {
                other is Undefined -> Undefined()
                other is Infinite && other == this -> Undefined()
                else -> this
            }

            override operator fun times(factor: Double): Duration =
                    if (factor == 0.0 || JDouble.isNaN(factor)) Undefined()
                    else if (factor < 0) minus(this)
                    else this

            override operator fun div(divisor: Double): Duration =
                    if (JDouble.isNaN(divisor) || divisor.isInfinite()) Undefined()
                    else if ((divisor.compareTo(0)) < 0) minus(this)
                    else this

            override operator fun div(divisor: Duration): Double = when (divisor) {
                is Infinite -> Double.NaN
                else -> Double.POSITIVE_INFINITY * (if ((this > Zero && divisor < Zero) || (this <= Zero && divisor >= Zero)) -1 else 1)
            }

            override fun isFinite() = false

            private fun fail(what: String): Nothing = throw IllegalArgumentException("$what not allowed on infinite Durations")
            override val length: Long = fail("length")
            override val unit: TimeUnit = fail("unit")
            override fun toNanos(): Long = fail("toNanos")
            override fun toMicros(): Long = fail("toMicros")
            override fun toMillis(): Long = fail("toMillis")
            override fun toSeconds(): Long = fail("toSeconds")
            override fun toMinutes(): Long = fail("toMinutes")
            override fun toHours(): Long = fail("toHours")
            override fun toDays(): Long = fail("toDays")

            override fun toCoarsest(): Duration = this

            /**
             * The Undefined value corresponds closely to Double.NaN:
             *
             *  - it is the result of otherwise invalid operations
             *  - it does not equal itself (according to `equals()`)
             *  - it compares greater than any other Duration apart from itself (for which `compare` returns 0)
             *
             * The particular comparison semantics mirror those of Double.NaN.
             *
             * '''''Use `eq` when checking an input of a method against this value.'''''
             */
            class Undefined : Infinite() {
                override fun toString(): String = "Duration.Undefined"
                override fun equals(other: Any?): Boolean = false
                override fun hashCode(): Int = super.hashCode()
                override fun plus(other: Duration): Duration = this
                override fun minus(other: Duration): Duration = this
                override fun times(factor: Double): Duration = this
                override fun div(divisor: Double): Duration = this
                override fun div(divisor: Duration): Double = Double.NaN

                override fun compareTo(other: Duration) = if (other == this) 0 else 1
                override operator fun unaryMinus(): Duration = this
                override fun toUnit(unit: TimeUnit): Double = Double.NaN

                private fun readResolve(): Any = Undefined() // Instructs deserialization to use this same instance
            }

            /**
             * Infinite duration: greater than any other (apart from Undefined) and not equal to any other
             * but itself. This value closely corresponds to Double.PositiveInfinity,
             * matching its semantics in arithmetic operations.
             */
            class Inf : Infinite() {
                override fun toString(): String = "Duration.Inf"
                override fun compareTo(other: Duration) = when (other) {
                    is Undefined -> -1 // Undefined != Undefined
                    this -> 0 // `case Inf` will include null checks in the byte code
                    else -> 1
                }

                override operator fun unaryMinus(): Duration = MinusInf()
                override fun toUnit(unit: TimeUnit): Double = Double.POSITIVE_INFINITY

                private fun readResolve(): Any = Inf() // Instructs deserialization to use this same instance
            }

            /**
             * Infinite duration: less than any other and not equal to any other
             * but itself. This value closely corresponds to Double.NegativeInfinity,
             * matching its semantics in arithmetic operations.
             */
            class MinusInf : Infinite() {
                override fun toString(): String = "Duration.MinusInf"
                override fun compareTo(other: Duration) = if (other == this) 0 else -1

                override operator fun unaryMinus(): Duration = Inf()
                override fun toUnit(unit: TimeUnit): Double = Double.NEGATIVE_INFINITY

                private fun readResolve(): Any = MinusInf() // Instructs deserialization to use this same instance
            }
        }

        // Java Factories

        /**
         * Construct a finite duration from the given length and time unit. The unit given is retained
         * throughout calculations as long as possible, so that it can be retrieved later.
         */
        fun create(length: Long, unit: TimeUnit): FiniteDuration = Duration(length, unit)

        /**
         * Construct a Duration from the given length and unit. Observe that nanosecond precision may be lost if
         *
         *  - the unit is NANOSECONDS
         *  - and the length has an absolute value greater than 2^53
         *
         * Infinite inputs (and NaN) are converted into [[Duration.Inf]], [[Duration.MinusInf]] and [[Duration.Undefined]], respectively.
         *
         * @throws IllegalArgumentException if the length was finite but the resulting duration cannot be expressed as a [[FiniteDuration]]
         */
        fun create(length: Double, unit: TimeUnit): Duration = Duration(length, unit)

        /**
         * Construct a finite duration from the given length and time unit, where the latter is
         * looked up in a list of string representation. Valid choices are:
         *
         * `d, day, h, hour, min, minute, s, sec, second, ms, milli, millisecond, µs, micro, microsecond, ns, nano, nanosecond`
         * and their pluralized forms (for every but the first mentioned form of each unit, i.e. no "ds", but "days").
         */
        fun create(length: Long, unit: String): FiniteDuration = Duration(length, unit)

        /**
         * Parse String into Duration.  Format is `"<length><unit>"`, where
         * whitespace is allowed before, between and after the parts. Infinities are
         * designated by `"Inf"`, `"PlusInf"`, `"+Inf"` and `"-Inf"` or `"MinusInf"`.
         *
         * @throws NumberFormatException if format is not parsable
         */
        fun create(s: String): Duration = Duration(s)

        /**
         * The natural ordering of durations matches the natural ordering for Double, including non-finite values.
         */
        object DurationIsOrdered : Comparator<Duration> {
            override fun compare(o1: Duration, o2: Duration): Int = o1.compareTo(o2)
        }
    }

    /**
     * Obtain the length of this Duration measured in the unit obtained by the `unit` method.
     *
     * $exc
     */
    abstract val length: Long

    /**
     * Obtain the time unit in which the length of this duration is measured.
     *
     * $exc
     */
    abstract val unit: TimeUnit

    /**
     * Return the length of this duration measured in whole nanoseconds, rounding towards zero.
     *
     * $exc
     */
    abstract fun toNanos(): Long

    /**
     * Return the length of this duration measured in whole microseconds, rounding towards zero.
     *
     * $exc
     */
    abstract fun toMicros(): Long

    /**
     * Return the length of this duration measured in whole milliseconds, rounding towards zero.
     *
     * $exc
     */
    abstract fun toMillis(): Long

    /**
     * Return the length of this duration measured in whole seconds, rounding towards zero.
     *
     * $exc
     */
    abstract fun toSeconds(): Long

    /**
     * Return the length of this duration measured in whole minutes, rounding towards zero.
     *
     * $exc
     */
    abstract fun toMinutes(): Long

    /**
     * Return the length of this duration measured in whole hours, rounding towards zero.
     *
     * $exc
     */
    abstract fun toHours(): Long

    /**
     * Return the length of this duration measured in whole days, rounding towards zero.
     *
     * $exc
     */
    abstract fun toDays(): Long

    /**
     * Return the number of nanoseconds as floating point number, scaled down to the given unit.
     * The result may not precisely represent this duration due to the Double datatype's inherent
     * limitations (mantissa size effectively 53 bits). Non-finite durations are represented as
     *  - [[Duration.Undefined]] is mapped to Double.NaN
     *  - [[Duration.Inf]] is mapped to Double.PositiveInfinity
     *  - [[Duration.MinusInf]] is mapped to Double.NegativeInfinity
     */
    abstract fun toUnit(unit: TimeUnit): Double

    /**
     * Return the sum of that duration and this. When involving non-finite summands the semantics match those
     * of Double.
     *
     * $ovf
     */
    abstract operator fun plus(other: Duration): Duration

    /**
     * Return the difference of that duration and this. When involving non-finite summands the semantics match those
     * of Double.
     *
     * $ovf
     */
    abstract operator fun minus(other: Duration): Duration

    /**
     * Return this duration multiplied by the scalar factor. When involving non-finite factors the semantics match those
     * of Double.
     *
     * $ovf
     */
    abstract operator fun times(factor: Double): Duration

    /**
     * Return this duration divided by the scalar factor. When involving non-finite factors the semantics match those
     * of Double.
     *
     * $ovf
     */
    abstract operator fun div(divisor: Double): Duration

    /**
     * Return the quotient of this and that duration as floating-point number. The semantics are
     * determined by Double as if calculating the quotient of the nanosecond lengths of both factors.
     */
    abstract operator fun div(divisor: Duration): Double

    /**
     * Negate this duration. The only two values which are mapped to themselves are [[Duration.Zero]] and [[Duration.Undefined]].
     */
    abstract operator fun unaryMinus(): Duration

    /**
     * This method returns whether this duration is finite, which is not the same as
     * `!isInfinite` for Double because this method also returns `false` for [[Duration.Undefined]].
     */
    abstract fun isFinite(): Boolean

    /**
     * Return the smaller of this and that duration as determined by the natural ordering.
     */
    fun min(other: Duration): Duration = if (this < other) this else other

    /**
     * Return the larger of this and that duration as determined by the natural ordering.
     */
    fun max(other: Duration): Duration = if (this > other) this else other

    // Java API

    /**
     * Return this duration divided by the scalar factor. When involving non-finite factors the semantics match those
     * of Double.
     *
     * $ovf
     */
    fun divide(divisor: Double) = this / divisor

    /**
     * Return the quotient of this and that duration as floating-point number. The semantics are
     * determined by Double as if calculating the quotient of the nanosecond lengths of both factors.
     */
    fun divide(other: Duration) = this / other

    fun gt(other: Duration) = this > other

    fun gteq(other: Duration) = this >= other

    fun lt(other: Duration) = this < other

    fun lteq(other: Duration) = this <= other

    /**
     * Return the difference of that duration and this. When involving non-finite summands the semantics match those
     * of Double.
     *
     * $ovf
     */
    fun minusDuration(other: Duration) = this - other

    /**
     * Return this duration multiplied by the scalar factor. When involving non-finite factors the semantics match those
     * of Double.
     *
     * $ovf
     */
    fun mul(factor: Double) = this * factor

    /**
     * Negate this duration. The only two values which are mapped to themselves are [[Duration.Zero]] and [[Duration.Undefined]].
     */
    fun neg() = -this

    /**
     * Return the sum of that duration and this. When involving non-finite summands the semantics match those
     * of Double.
     *
     * $ovf
     */
    fun plusDuration(other: Duration) = this + other

    /**
     * Return duration which is equal to this duration but with a coarsest Unit, or self in case it is already the coarsest Unit
     * <p/>
     * Examples:
     * {{{
     * Duration(60, MINUTES).toCoarsest // Duration(1, HOURS)
     * Duration(1000, MILLISECONDS).toCoarsest // Duration(1, SECONDS)
     * Duration(48, HOURS).toCoarsest // Duration(2, DAYS)
     * Duration(5, SECONDS).toCoarsest // Duration(5, SECONDS)
     * }}}
     */
    abstract fun toCoarsest(): Duration
}

/**
 * This class represents a finite duration. Its addition and subtraction operators are overloaded to retain
 * this guarantee statically. The range of this class is limited to +-(2^63-1)ns, which is roughly 292 years.
 */
class FiniteDuration(override val length: Long, override val unit: TimeUnit) : Duration() {
    override fun compareTo(other: Duration): Int = when (other) {
        is FiniteDuration -> toNanos().compareTo(other.toNanos())
        else -> -(other.compareTo(this))
    }

    companion object {

        val COMPARATOR = java.util.Comparator<FiniteDuration> { o1, o2 -> o1.compareTo(o2) }

        inline operator fun invoke(length: Long, unit: TimeUnit): FiniteDuration = FiniteDuration(length, unit)

        inline operator fun invoke(length: Long, unit: String): FiniteDuration = FiniteDuration(length, Duration.timeUnit[unit]!!)

        // limit on abs. value of durations in their units
        private val max_ns = Long.MAX_VALUE
        private val max_µs = max_ns / 1000
        private val max_ms = max_µs / 1000
        private val max_s = max_ms / 1000
        private val max_min = max_s / 60
        private val max_h = max_min / 60
        private val max_d = max_h / 24
    }

    private fun bounded(max: Long) = -max <= length && length <= max

    init {
        require(when (unit) {
            NANOSECONDS -> bounded(max_ns)
            MICROSECONDS -> bounded(max_µs)
            MILLISECONDS -> bounded(max_ms)
            SECONDS -> bounded(max_s)
            MINUTES -> bounded(max_min)
            HOURS -> bounded(max_h)
            DAYS -> bounded(max_d)
        }) { "Duration is limited to +-(2^63-1)ns (ca. 292 years)" }
    }

    override fun toNanos() = unit.toNanos(length)
    override fun toMicros() = unit.toMicros(length)
    override fun toMillis() = unit.toMillis(length)
    override fun toSeconds() = unit.toSeconds(length)
    override fun toMinutes() = unit.toMinutes(length)
    override fun toHours() = unit.toHours(length)
    override fun toDays() = unit.toDays(length)
    override fun toUnit(unit: TimeUnit): Double = toNanos().toDouble() / NANOSECONDS.convert(1, unit)

    /**
     * Construct a [[Deadline]] from this duration by adding it to the current instant `Deadline.now`.
     */
    fun fromNow(): Deadline = Deadline.now() + this

    private fun unitString() = timeUnitName[unit] + (if (length == 1L) "" else "s")
    override fun toString(): String = "" + length + " " + unitString()

    // see https://www.securecoding.cert.org/confluence/display/java/NUM00-J.+Detect+or+prevent+integer+overflow
    private fun safeAdd(a: Long, b: Long): Long {
        if ((b > 0) && (a > Long.MAX_VALUE - b) ||
                (b < 0) && (a < Long.MIN_VALUE - b)) throw IllegalArgumentException("integer overflow")
        return a + b
    }

    private fun add(otherLength: Long, otherUnit: TimeUnit): FiniteDuration {
        val commonUnit = if (otherUnit.convert(1, unit) == 0L) unit else otherUnit
        val totalLength = safeAdd(commonUnit.convert(length, unit), commonUnit.convert(otherLength, otherUnit))
        return FiniteDuration(totalLength, commonUnit)
    }

    override operator fun plus(other: Duration) = when (other) {
        is FiniteDuration -> add(other.length, other.unit)
        else -> other
    }

    override operator fun minus(other: Duration) = when (other) {
        is FiniteDuration -> add(-other.length, other.unit)
        else -> -other
    }

    override operator fun times(factor: Double): Duration =
            if (!factor.isInfinite()) fromNanos(toNanos() * factor)
            else if (JDouble.isNaN(factor)) Duration.Companion.Infinite.Undefined()
            else if (((factor > 0) && this >= Zero) || (factor <= 0 && (this < Zero))) Infinite.Inf()
            else Infinite.MinusInf()

    override operator fun div(divisor: Double): Duration =
            if (!divisor.isInfinite()) fromNanos(toNanos() / divisor)
            else if (JDouble.isNaN(divisor)) Infinite.Undefined()
            else Zero

    // if this is made a constant, then scalac will elide the conditional and always return +0.0, scala/bug#6331
    private fun minusZero(): Double = -0.0

    override operator fun div(divisor: Duration): Double =
            if (divisor.isFinite()) toNanos().toDouble() / divisor.toNanos()
            else if (divisor == Infinite.Undefined()) Double.NaN
            else if ((length < 0 && divisor <= Zero) || length >= 0 && (divisor > Zero)) 0.0 else minusZero()

    // overloaded methods taking FiniteDurations, so that you can calculate while statically staying finite
    operator fun plus(other: FiniteDuration): FiniteDuration = add(other.length, other.unit)

    operator fun minus(other: FiniteDuration): FiniteDuration = add(-other.length, other.unit)

    fun plusFiniteDuration(other: FiniteDuration): FiniteDuration = this + other

    fun minusFiniteDuration(other: FiniteDuration): FiniteDuration = this - other

    fun min(other: FiniteDuration): FiniteDuration = if (this < other) this else other

    fun max(other: FiniteDuration): FiniteDuration = if (this > other) this else other

    // overloaded methods taking Long so that you can calculate while statically staying finite

    /**
     * Return the quotient of this duration and the given integer factor.
     *
     * @throws ArithmeticException if the factor is 0
     */
    operator fun div(divisor: Long) = fromNanos(toNanos() / divisor)

    /**
     * Return the product of this duration and the given integer factor.
     *
     * @throws IllegalArgumentException if the result would overflow the range of FiniteDuration
     */
    operator fun times(factor: Long) = FiniteDuration(safeMul(length, factor), unit)

    /*
     * This method avoids the use of Long division, which saves 95% of the time spent,
     * by checking that there are enough leading zeros so that the result has a chance
     * to fit into a Long again; the remaining edge cases are caught by using the sign
     * of the product for overflow detection.
     *
     * This method is not general purpose because it disallows the (otherwise legal)
     * case of Long.MinValue * 1, but that is okay for use in FiniteDuration, since
     * Long.MinValue is not a legal `length` anyway.
     */
    private fun safeMul(_a: Long, _b: Long): Long {
        val a = Math.abs(_a)
        val b = Math.abs(_b)

        if (leading(a) + leading(b) < 64) throw IllegalArgumentException("multiplication overflow")
        val product = a * b
        if (product < 0) throw IllegalArgumentException("multiplication overflow")
        return if (((a == _a) && b != _b) || (a != _a && (b == _b))) -product else product
    }

    /**
     * Return the quotient of this duration and the given integer factor.
     *
     * @throws ArithmeticException if the factor is 0
     */
    fun divLong(divisor: Long) = this / divisor

    /**
     * Return the product of this duration and the given integer factor.
     *
     * @throws IllegalArgumentException if the result would overflow the range of FiniteDuration
     */
    fun multLong(factor: Long) = this * factor

    override operator fun unaryMinus(): Duration = Duration(-length, unit)

    override fun isFinite() = true

    override fun toCoarsest(): FiniteDuration {
        fun loop(length: Long, unit: TimeUnit): FiniteDuration {
            fun coarserOrThis(coarser: TimeUnit, divider: Int) =
                    if (length % divider == 0L) loop(length / divider, coarser)
                    else if (unit == this.unit) this
                    else FiniteDuration(length, unit)

            return when (unit) {
                TimeUnit.DAYS -> FiniteDuration(length, unit)
                TimeUnit.HOURS -> coarserOrThis(DAYS, 24)
                TimeUnit.MINUTES -> coarserOrThis(HOURS, 60)
                TimeUnit.SECONDS -> coarserOrThis(MINUTES, 60)
                TimeUnit.MILLISECONDS -> coarserOrThis(SECONDS, 1000)
                TimeUnit.MICROSECONDS -> coarserOrThis(MILLISECONDS, 1000)
                TimeUnit.NANOSECONDS -> coarserOrThis(MICROSECONDS, 1000)
            }
        }

        return if (unit == DAYS || length == 0L) this else loop(length, unit)
    }

    override fun equals(other: Any?): Boolean = when (other) {
        is FiniteDuration -> toNanos() == other.toNanos()
        else -> this === other
    }

    override fun hashCode() = toNanos().toInt()
}
