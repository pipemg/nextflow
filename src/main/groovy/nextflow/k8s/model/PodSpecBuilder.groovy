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

package nextflow.k8s.model

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger

import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import nextflow.util.MemoryUnit
/**
 * Object build for a K8s pod specification
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class PodSpecBuilder {

    static @PackageScope AtomicInteger VOLUMES = new AtomicInteger()

    String podName

    String imageName

    String pullPolicy

    List<String> command = []

    Map<String,String> labels = [:]

    String namespace

    String restart

    List<PodEnv> envVars = []

    String workDir

    Integer cpus

    String memory

    String serviceAccount

    Collection<PodMountSecret> secrets = []

    Collection<PodMountConfig> configMaps = []

    Collection<PodHostMount> hostMounts = []

    Collection<PodVolumeClaim> volumeClaims = []


    /**
     * @return A sequential volume unique identifier
     */
    static protected String nextVolName() {
        "vol-${VOLUMES.incrementAndGet()}".toString()
    }


    PodSpecBuilder withPodName(String name) {
        this.podName = name
        return this
    }

    PodSpecBuilder withImageName(String name) {
        this.imageName = name
        return this
    }

    PodSpecBuilder withPullPolicy(String policy) {
        this.pullPolicy = policy
        return this
    }

    PodSpecBuilder withWorkDir( String path ) {
        this.workDir = path
        return this
    }

    PodSpecBuilder withWorkDir(Path path ) {
        this.workDir = path.toString()
        return this
    }

    PodSpecBuilder withNamespace(String name) {
        this.namespace = name
        return this
    }

    PodSpecBuilder withServiceAccount(String name) {
        this.serviceAccount = name
        return this
    }

    PodSpecBuilder withCommand( cmd ) {
        assert cmd instanceof List || cmd instanceof CharSequence, "Missing or invalid K8s command parameter: $cmd"
        this.command = cmd instanceof List ? cmd : ['/bin/bash','-c', cmd.toString()]
        return this
    }

    PodSpecBuilder withCpus( Integer cpus ) {
        this.cpus = cpus
        return this
    }

    PodSpecBuilder withMemory(String mem) {
        this.memory = mem
        return this
    }

    PodSpecBuilder withMemory(MemoryUnit mem)  {
        this.memory = "${mem.mega}Mi".toString()
        return this
    }

    PodSpecBuilder withLabel( String name, String value ) {
        this.labels.put(name, value)
        return this
    }

    PodSpecBuilder withLabels(Map labels) {
        this.labels.putAll(labels)
        return this
    }


    PodSpecBuilder withEnv( PodEnv var ) {
        envVars.add(var)
        return this
    }

    PodSpecBuilder withEnv( Collection vars ) {
        envVars.addAll(vars)
        return this
    }

    PodSpecBuilder withVolumeClaim( String name, String mount ) {
        volumeClaims.add(new PodVolumeClaim(name, mount))
        return this
    }

    PodSpecBuilder withVolumeClaim( PodVolumeClaim claim ) {
        volumeClaims.add(claim)
        return this
    }

    PodSpecBuilder withVolumeClaims( Collection<PodVolumeClaim> claims ) {
        volumeClaims.addAll(claims)
        return this
    }

    PodSpecBuilder withConfigMaps( Collection<PodMountConfig> configMaps ) {
        this.configMaps.addAll(configMaps)
        return this
    }

    PodSpecBuilder withConfigMap( PodMountConfig configMap ) {
        this.configMaps.add(configMap)
        return this
    }

    PodSpecBuilder withSecrets( Collection<PodMountSecret> secrets ) {
        this.secrets.addAll(secrets)
        return this
    }

    PodSpecBuilder withSecret( PodMountSecret secret ) {
        this.secrets.add(secret)
        return this
    }

    PodSpecBuilder withHostMounts( Collection<PodHostMount> mounts ) {
        this.hostMounts.addAll(mounts)
        return this
    }

    PodSpecBuilder withHostMount( String host, String mount ) {
        this.hostMounts.add( new PodHostMount(host, mount))
        return this
    }

    PodSpecBuilder withPodOptions(PodOptions opts) {
        // -- pull policy
        if( opts.pullPolicy )
            pullPolicy = opts.pullPolicy
        // -- env vars
        if( opts.getEnvVars() )
            envVars.addAll( opts.getEnvVars() )
        // -- secrets
        if( opts.getMountSecrets() )
            secrets.addAll( opts.getMountSecrets() )
        // -- configMaps
        if( opts.getMountConfigMaps() )
            configMaps.addAll( opts.getMountConfigMaps() )
        // -- volume claims 
        if( opts.getVolumeClaims() )
            volumeClaims.addAll( opts.getVolumeClaims() )
        return this
    }

    Map build() {
        assert this.podName, 'Missing K8s podName parameter'
        assert this.imageName, 'Missing K8s imageName parameter'
        assert this.command, 'Missing K8s command parameter'

        final restart = this.restart ?: 'Never'

        final metadata = new LinkedHashMap<String,Object>()
        metadata.name = podName
        metadata.namespace = namespace ?: 'default'

        final labels = this.labels ?: [:]
        final env = []
        for( PodEnv entry : this.envVars ) {
            env.add(entry.toSpec())
        }

        final res = [:]
        if( this.cpus )
            res.cpu = this.cpus
        if( this.memory )
            res.memory = this.memory

        final container = [
                name: this.podName,
                image: this.imageName,
                command: this.command
        ]
        
        if( this.workDir )
            container.put('workingDir', workDir)

        final spec = [
                restartPolicy: restart,
                containers: [ container ],
        ]

        if( this.serviceAccount )
            spec.serviceAccountName = this.serviceAccount

        // add labels
        if( labels )
            metadata.labels = labels

        final pod = [
                apiVersion: 'v1',
                kind: 'Pod',
                metadata: metadata,
                spec: spec
        ]

        // add environment
        if( env )
            container.env = env

        // add resources
        if( res ) {
            Map limits = [limits: res]
            container.resources = limits
        }

        // add storage definitions ie. volumes and mounts
        final mounts = []
        final volumes = []

        // -- volume claims
        for( PodVolumeClaim entry : volumeClaims ) {
            final name = nextVolName()
            final claim = [name: name, mountPath: entry.mountPath ]
            mounts << claim
            volumes << [name: name, persistentVolumeClaim: [claimName: entry.claimName]]
        }

        // -- configMap volumes
        for( PodMountConfig entry : configMaps ) {
            final name = nextVolName()
            configMapToSpec(name, entry, mounts, volumes)
        }

        // host mounts
        for( PodHostMount entry : hostMounts ) {
            final name = nextVolName()
            mounts << [name: name, mountPath: entry.mountPath]
            volumes << [name: name, hostPath: [path: entry.hostPath]]
        }

        // secret volumes
        for( PodMountSecret entry : secrets ) {
            final name = nextVolName()
            secretToSpec(name, entry, mounts, volumes)
        }


        if( volumes )
            spec.volumes = volumes
        if( mounts )
            container.volumeMounts = mounts

        return pod
    }

    @PackageScope
    @CompileDynamic
    static void secretToSpec(String volName, PodMountSecret entry, List mounts, List volumes ) {
        assert entry

        final secret = [secretName: entry.secretName]
        if( entry.secretKey ) {
            secret.items = [ [key: entry.secretKey, path: entry.fileName ] ]
        }

        mounts << [name: volName, mountPath: entry.mountPath]
        volumes << [name: volName, secret: secret ]
    }

    @PackageScope
    @CompileDynamic
    static void configMapToSpec(String volName, PodMountConfig entry, List<Map> mounts, List<Map> volumes ) {
        assert entry

        final config = [name: entry.configName]
        if( entry.configKey ) {
            config.items = [ [key: entry.configKey, path: entry.fileName ] ]
        }

        mounts << [name: volName, mountPath: entry.mountPath]
        volumes << [name: volName, configMap: config ]
    }


}
