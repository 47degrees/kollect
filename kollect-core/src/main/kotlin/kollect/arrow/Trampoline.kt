package kollect.arrow

import arrow.effects.internal.Platform


/**
 * Trampoline implementation, meant to be stored in a `ThreadLocal`.
 * See `TrampolineEC`.
 *
 * INTERNAL API.
 */
open class Trampoline(private val underlying: ExecutionContext) {

    private var immediateQueue = Platform.ArrayStack<Runnable>()
    private var withinLoop = false

    open fun startLoop(runnable: Runnable): Unit {
        withinLoop = true
        try {
            immediateLoop(runnable)
        } finally {
            withinLoop = false
        }
    }

    fun execute(runnable: Runnable): Unit {
        if (!withinLoop) {
            startLoop(runnable)
        } else {
            immediateQueue.push(runnable)
        }
    }

    open fun forkTheRest(): Unit {
        class ResumeRun(val head: Runnable, val rest: Platform.ArrayStack<Runnable>) : Runnable {
            override fun run() {
                val itr = rest.iterator()
                while (itr.hasNext()) {
                    immediateQueue.push(itr.next())
                }
                immediateLoop(head)
            }
        }

        val head = immediateQueue.pop()
        if (head != null) {
            val rest = immediateQueue
            immediateQueue = Platform.ArrayStack()
            underlying.execute(ResumeRun(head, rest))
        }
    }

    private fun immediateLoop(task: Runnable): Unit {
        try {
            task.run()
        } catch (ex: Exception) {
            forkTheRest()
            if (NonFatal(ex)) underlying.reportFailure(ex)
            else throw ex
        }

        val next = immediateQueue.pop()
        if (next != null) immediateLoop(next)
    }
}