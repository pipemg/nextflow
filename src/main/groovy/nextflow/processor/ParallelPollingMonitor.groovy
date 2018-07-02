/*
 * Copyright (c) 2013-2018, Centre for Genomic Regulation (CRG).
 * Copyright (c) 2013-2018, Paolo Di Tommaso and the respective authors.
 *
 *   This file is part of 'Nextflow'.
 *
 *   Nextflow is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   Nextflow is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with Nextflow.  If not, see <http://www.gnu.org/licenses/>.
 */

package nextflow.processor

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.util.Duration
/**
 * Polling monitor class submitting job execution
 * is a parallel manner
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class ParallelPollingMonitor extends TaskPollingMonitor {

    private ExecutorService executorService

    static ParallelPollingMonitor create(Session session, String name, Duration defPollInterval) {
        assert session
        assert name

        final pollInterval = session.getPollInterval(name, defPollInterval)
        final dumpInterval = session.getMonitorDumpInterval(name)
        final capacity = session.getQueueSize(name, 0)

        log.debug "Creating parallel monitor for executor '$name' > pollInterval=$pollInterval; dumpInterval=$dumpInterval; capacity=$capacity"
        new ParallelPollingMonitor(name: name, session: session, pollInterval: pollInterval, dumpInterval: dumpInterval, capacity: capacity)
    }


    /**
     * Create the task polling monitor with the provided named parameters object.
     * <p>
     * Valid parameters are:
     * <li>name: The name of the executor for which the polling monitor is created
     * <li>session: The current {@code Session}
     * <li>capacity: The maximum number of this monitoring queue
     * <li>pollInterval: Determines how often a poll occurs to check for a process termination
     * <li>dumpInterval: Determines how often the executor status is written in the application log file
     *
     * @param params
     */
    protected ParallelPollingMonitor(Map params) {
        super(params)
    }

    @Override
    TaskMonitor start() {
        createExecutorService()
        super.start()
    }

    private void createExecutorService() {
        executorService = Executors.newCachedThreadPool()
        if(!session)
            return
        session.onShutdown {
            executorService.shutdown()
        }
    }

    @Override
    protected void submit(TaskHandler handler) {
        // execute task submission in a parallel
        // using an thread-pool via the executor service
        executorService.execute({

            try {
                super.submit(handler)
            }
            catch (Throwable e) {
                handleException(handler, e)
                session.notifyTaskComplete(handler)
            }

        } as Runnable)
    }
}
