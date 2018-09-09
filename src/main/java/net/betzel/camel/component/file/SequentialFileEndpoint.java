/*
 *
 * Copyright 2018 Maurice Betzel
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.betzel.camel.component.file;

import org.apache.camel.Component;
import org.apache.camel.component.file.*;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.util.ObjectHelper;

import java.io.File;

/**
 * The file component is used for reading or writing files.
 */
@UriEndpoint(firstVersion = "2.21.2", scheme = "seqFile", title = "SequentialFile", syntax = "seqFile:directoryName", consumerClass = FileConsumer.class, label = "core,file")
public class SequentialFileEndpoint extends FileEndpoint {

    private final FileOperations operations = new FileOperations(this);

    public SequentialFileEndpoint() {
    }

    public SequentialFileEndpoint(String endpointUri, Component component) {
        super(endpointUri, component);
    }

    @Override
    public GenericFileProducer<File> createProducer() throws Exception {
        ObjectHelper.notNull(operations, "operations");

        // you cannot use temp file and file exists append
        if (getFileExist() == GenericFileExist.Append && ((getTempPrefix() != null) || (getTempFileName() != null))) {
            throw new IllegalArgumentException("You cannot set both fileExist=Append and tempPrefix/tempFileName options");
        }

        // ensure fileExist and moveExisting is configured correctly if in use
        if (getFileExist() == GenericFileExist.Move && getMoveExisting() == null) {
            throw new IllegalArgumentException("You must configure moveExisting option when fileExist=Move");
        } else if (getMoveExisting() != null && getFileExist() != GenericFileExist.Move) {
            throw new IllegalArgumentException("You must configure fileExist=Move when moveExisting has been set");
        }

        return new GenericSequentialFileProducer(this, operations);
    }

}
