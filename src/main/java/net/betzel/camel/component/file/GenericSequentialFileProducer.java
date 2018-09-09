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

import org.apache.camel.Exchange;
import org.apache.camel.ExpressionIllegalSyntaxException;
import org.apache.camel.component.file.*;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.util.FileUtil;
import org.apache.camel.util.ObjectHelper;
import org.apache.camel.util.StringHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.regex.Matcher;

import static net.betzel.camel.component.file.SequentialFileComponent.PREVIOUS_FILE_NAME;

public class GenericSequentialFileProducer extends GenericFileProducer {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected GenericSequentialFileProducer(GenericFileEndpoint endpoint, GenericFileOperations operations) {
        super(endpoint, operations);
    }

    @Override
    protected void processExchange(Exchange exchange, String target) throws Exception {
        log.trace("Processing file: {} for exchange: {}", target, exchange);

        try {
            preWriteCheck(exchange, target);

            // should we write to a temporary name and then afterwards rename to real target
            boolean writeAsTempAndRename = ObjectHelper.isNotEmpty(endpoint.getTempFileName());
            String tempTarget = null;
            // remember if target exists to avoid checking twice
            Boolean targetExists;
            if (writeAsTempAndRename) {
                // compute temporary name with the temp prefix
                tempTarget = createTempFileName(exchange, target);

                log.trace("Writing using tempNameFile: {}", tempTarget);

                //if we should eager delete target file before deploying temporary file
                if (endpoint.getFileExist() != GenericFileExist.TryRename && endpoint.isEagerDeleteTargetFile()) {

                    // cater for file exists option on the real target as
                    // the file operations code will work on the temp file

                    // if an existing file already exists what should we do?
                    targetExists = operations.existsFile(target);
                    if (targetExists) {

                        log.trace("EagerDeleteTargetFile, target exists");

                        if (endpoint.getFileExist() == GenericFileExist.Ignore) {
                            // ignore but indicate that the file was written
                            log.trace("An existing file already exists: {}. Ignore and do not override it.", target);
                            return;
                        } else if (endpoint.getFileExist() == GenericFileExist.Fail) {
                            throw new GenericFileOperationFailedException("File already exist: " + target + ". Cannot write new file.");
                        } else if (endpoint.getFileExist() == GenericFileExist.Move) {
                            // move any existing file first
                            doMoveExistingFile(target);
                        } else if (endpoint.isEagerDeleteTargetFile() && endpoint.getFileExist() == GenericFileExist.Override) {
                            // we override the target so we do this by deleting it so the temp file can be renamed later
                            // with success as the existing target file have been deleted
                            log.trace("Eagerly deleting existing file: {}", target);
                            if (!operations.deleteFile(target)) {
                                throw new GenericFileOperationFailedException("Cannot delete file: " + target);
                            }
                        }
                    }
                }

                // delete any pre existing temp file
                if (endpoint.getFileExist() != GenericFileExist.TryRename && operations.existsFile(tempTarget)) {
                    log.trace("Deleting existing temp file: {}", tempTarget);
                    if (!operations.deleteFile(tempTarget)) {
                        throw new GenericFileOperationFailedException("Cannot delete file: " + tempTarget);
                    }
                }
            }

            // write/upload the file
            writeFile(exchange, tempTarget != null ? tempTarget : target);

            // if we did write to a temporary name then rename it to the real
            // name after we have written the file
            if (tempTarget != null) {
                // if we did not eager delete the target file
                if (endpoint.getFileExist() != GenericFileExist.TryRename && !endpoint.isEagerDeleteTargetFile()) {

                    // if an existing file already exists what should we do?
                    targetExists = operations.existsFile(target);
                    if (targetExists) {

                        log.trace("Not using EagerDeleteTargetFile, target exists");

                        if (endpoint.getFileExist() == GenericFileExist.Ignore) {
                            // ignore but indicate that the file was written
                            log.trace("An existing file already exists: {}. Ignore and do not override it.", target);
                            return;
                        } else if (endpoint.getFileExist() == GenericFileExist.Fail) {
                            throw new GenericFileOperationFailedException("File already exist: " + target + ". Cannot write new file.");
                        } else if (endpoint.getFileExist() == GenericFileExist.Override) {
                            // we override the target so we do this by deleting it so the temp file can be renamed later
                            // with success as the existing target file have been deleted
                            log.trace("Deleting existing file: {}", target);
                            if (!operations.deleteFile(target)) {
                                throw new GenericFileOperationFailedException("Cannot delete file: " + target);
                            }
                        }
                    }
                }

                // now we are ready to rename the temp file to the target file
                log.trace("Renaming file: [{}] to: [{}]", tempTarget, target);
                boolean renamed = operations.renameFile(tempTarget, target);
                if (!renamed) {
                    throw new GenericFileOperationFailedException("Cannot rename file from: " + tempTarget + " to: " + target);
                }
            }

            // any done file to write?
            if (endpoint.getDoneFileName() != null) {
                String doneFileName = createDoneFileName(target);
                ObjectHelper.notEmpty(doneFileName, "doneFileName", endpoint);

                // create empty exchange with empty body to write as the done file
                Exchange empty = new DefaultExchange(exchange);
                empty.getIn().setBody("");

                log.trace("Writing done file: [{}]", doneFileName);
                // delete any existing done file
                if (operations.existsFile(doneFileName)) {
                    if (!operations.deleteFile(doneFileName)) {
                        throw new GenericFileOperationFailedException("Cannot delete existing done file: " + doneFileName);
                    }
                }
                writeFile(empty, doneFileName);
            }

            // let's store the name we really used in the header, so end-users
            // can retrieve it
            exchange.getIn().setHeader(Exchange.FILE_NAME_PRODUCED, target);
        } catch (Exception e) {
            handleFailedWrite(exchange, e);
        }

        postWriteCheck(exchange);
    }

    /**
     * Detect if previous file is still present
     */

    public void preWriteCheck(Exchange exchange, String fileName) throws GenericFileOperationFailedException {
        String previousFileName = exchange.getIn().getHeader(PREVIOUS_FILE_NAME, String.class);
        if(previousFileName != null) {
            String path = FileUtil.onlyPath(fileName) + File.separatorChar + previousFileName;
            if(operations.existsFile(path)) {
                throw new GenericFileOperationFailedException("File still exists: " + previousFileName);
            }
        }
    }

    private void doMoveExistingFile(String fileName) throws GenericFileOperationFailedException {
        // need to evaluate using a dummy and simulate the file first, to have access to all the file attributes
        // create a dummy exchange as Exchange is needed for expression evaluation
        // we support only the following 3 tokens.
        Exchange dummy = endpoint.createExchange();
        String parent = FileUtil.onlyPath(fileName);
        String onlyName = FileUtil.stripPath(fileName);
        dummy.getIn().setHeader(Exchange.FILE_NAME, fileName);
        dummy.getIn().setHeader(Exchange.FILE_NAME_ONLY, onlyName);
        dummy.getIn().setHeader(Exchange.FILE_PARENT, parent);

        String to = endpoint.getMoveExisting().evaluate(dummy, String.class);
        // we must normalize it (to avoid having both \ and / in the name which confuses java.io.File)
        to = FileUtil.normalizePath(to);
        if (ObjectHelper.isEmpty(to)) {
            throw new GenericFileOperationFailedException("moveExisting evaluated as empty String, cannot move existing file: " + fileName);
        }

        boolean renamed = operations.renameFile(fileName, to);
        if (!renamed) {
            throw new GenericFileOperationFailedException("Cannot rename file from: " + fileName + " to: " + to);
        }
    }

    /**
     * Creates the associated name of the done file based on the given file name.
     * <p/>
     * This method should only be invoked if a done filename property has been set on this endpoint.
     *
     * @param fileName the file name
     * @return name of the associated done file name
     */
    protected String createDoneFileName(String fileName) {
        String pattern = endpoint.getDoneFileName();
        ObjectHelper.notEmpty(pattern, "doneFileName", pattern);

        // we only support ${file:name} or ${file:name.noext} as dynamic placeholders for done files
        String path = FileUtil.onlyPath(fileName);
        String onlyName = Matcher.quoteReplacement(FileUtil.stripPath(fileName));

        pattern = pattern.replaceFirst("\\$\\{file:name\\}", onlyName);
        pattern = pattern.replaceFirst("\\$simple\\{file:name\\}", onlyName);
        pattern = pattern.replaceFirst("\\$\\{file:name.noext\\}", FileUtil.stripExt(onlyName));
        pattern = pattern.replaceFirst("\\$simple\\{file:name.noext\\}", FileUtil.stripExt(onlyName));

        // must be able to resolve all placeholders supported
        if (StringHelper.hasStartToken(pattern, "simple")) {
            throw new ExpressionIllegalSyntaxException(fileName + ". Cannot resolve reminder: " + pattern);
        }

        String answer = pattern;
        if (ObjectHelper.isNotEmpty(path) && ObjectHelper.isNotEmpty(pattern)) {
            // done file must always be in same directory as the real file name
            answer = path + getFileSeparator() + pattern;
        }

        if (endpoint.getConfiguration().needToNormalize()) {
            // must normalize path to cater for Windows and other OS
            answer = FileUtil.normalizePath(answer);
        }

        return answer;
    }

}