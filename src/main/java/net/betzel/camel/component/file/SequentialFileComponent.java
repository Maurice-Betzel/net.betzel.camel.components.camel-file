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

import org.apache.camel.CamelContext;
import org.apache.camel.component.file.*;
import org.apache.camel.util.FileUtil;
import org.apache.camel.util.StringHelper;

import java.io.File;
import java.util.Map;

public class SequentialFileComponent extends FileComponent {

    /**
     * Previous file name header.
     */
    public static final String PREVIOUS_FILE_NAME = "CamelPreviousFileName";

    public SequentialFileComponent() {
    }

    public SequentialFileComponent(CamelContext context) {
        super(context);
    }

    @Override
    protected GenericFileEndpoint<File> buildFileEndpoint(String uri, String remaining, Map<String, Object> parameters) {
        // the starting directory must be a static (not containing dynamic expressions)
        if (StringHelper.hasStartToken(remaining, "simple")) {
            throw new IllegalArgumentException("Invalid directory: " + remaining
                    + ". Dynamic expressions with ${ } placeholders is not allowed."
                    + " Use the fileName option to set the dynamic expression.");
        }

        File file = new File(remaining);

        SequentialFileEndpoint result = new SequentialFileEndpoint(uri, this);
        result.setFile(file);

        GenericFileConfiguration config = new GenericFileConfiguration();
        config.setDirectory(FileUtil.isAbsolute(file) ? file.getAbsolutePath() : file.getPath());
        result.setConfiguration(config);

        return result;
    }

}