/*
 *
 * Copyright BinquanWang
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
 *
 */

package org.wbq.spring.boot.autoconfigure.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.wbq.spring.boot.autoconfigure.properties.HadoopProperities;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class HdfsTool {
    private Log LOG = LogFactory.getLog(getClass());
    private FileSystem fileSystem = null;

    public HdfsTool(org.apache.hadoop.conf.Configuration configuration,
                    HadoopProperities hadoopProperities) throws IOException {
        String hadoopURI = "";
        String uri = hadoopProperities.hadoopHdfsUri();
        if (uri != null && uri.trim().startsWith("hdfs://")) {
            hadoopURI = uri.trim();
            if (!(uri.trim().endsWith("/"))) {
                hadoopURI += "/";
            }
        }
        if (!hadoopURI.equals("")) {
            configuration.set(FileSystem.FS_DEFAULT_NAME_KEY, hadoopURI);
            LOG.info("Set " + FileSystem.FS_DEFAULT_NAME_KEY +
                    " = [" + hadoopURI + "] to configuration");
        }
        LOG.info("Create fileSystem end, the " +
                FileSystem.FS_DEFAULT_NAME_KEY +
                " = [" + configuration.get(FileSystem.FS_DEFAULT_NAME_KEY) + "]");
        this.fileSystem = FileSystem.get(configuration);
    }

    public boolean getOrCreatePath(String path) {
        Path pathObj = new Path(path);
        try {
            boolean existFlag = fileSystem.exists(pathObj);
            if (!existFlag) {
                boolean result = fileSystem.mkdirs(pathObj);
                if (!result) {
                    return false;
                }
            }
        } catch (IOException e) {
            return false;
        }
        return true;
    }

    public boolean copyFileToHdfs(String srcDiskPath, String distHdfsPath) throws IOException {
        return copyFileToHdfs(srcDiskPath, distHdfsPath, true, false);
    }

    public boolean copyFileToHdfs(String srcDiskPath,
                                  String distHdfsPath,
                                  boolean overwriteIfExist,
                                  boolean delSrcFile) throws IOException {
        return FileUtil.copy(
                FileSystem.getLocal(fileSystem.getConf()),
                new Path(srcDiskPath),
                fileSystem, new Path(distHdfsPath),
                delSrcFile,
                overwriteIfExist,
                fileSystem.getConf()
        );
    }

    public boolean copyFileFromHdfs(String srcHdfsPath, String distDiskPath) throws IOException {
        return copyFileFromHdfs(srcHdfsPath, distDiskPath, true, false);
    }

    public boolean copyFileFromHdfs(String srcHdfsPath,
                                    String distDiskPath,
                                    boolean overwriteIfExist,
                                    boolean delSrcFile) throws IOException {
        return FileUtil.copy(
                fileSystem,
                new Path(srcHdfsPath),
                FileSystem.getLocal(fileSystem.getConf()),
                new Path(distDiskPath),
                delSrcFile,
                overwriteIfExist,
                fileSystem.getConf());
    }

    public InputStream getHdfsFileInputStream(String filePath) throws IOException {
        return fileSystem.open(new Path(filePath));
    }

    public OutputStream getHdfsFileOutputStream(String filePath, boolean appendOnExist) throws IOException {
        if (appendOnExist) {
            if (fileSystem.exists(new Path(filePath))) {
                return fileSystem.append(new Path(filePath));
            } else {
                return fileSystem.create(new Path(filePath), true);
            }
        } else {
            return fileSystem.create(new Path(filePath), true);
        }
    }

    public boolean deleteHdfsFile(String filePath, boolean deleteIfDirectory) throws IOException {
        return fileSystem.delete(new Path(filePath), deleteIfDirectory);
    }

    public boolean isFile(String path) throws IOException {
        return fileSystem.isFile(new Path(path));
    }

    public boolean isDictionary(String path) throws IOException {
        return fileSystem.isDirectory(new Path(path));
    }

    public boolean exist(String path) throws IOException {
        return fileSystem.exists(new Path(path));
    }

    public String[] getAllFileInDirctory(String path, boolean recursive) throws IOException {
        RemoteIterator<LocatedFileStatus> fileIterator = fileSystem.listFiles(new Path(path), recursive);
        List<String> fileOut = new ArrayList<String>();
        while (fileIterator.hasNext()) {
            LocatedFileStatus file = fileIterator.next();
            fileOut.add(file.getPath().toString());
        }
        return fileOut.toArray(new String[fileOut.size()]);
    }

    /**
     * Don't invoke the close() method;
     *
     * @return fileSystem
     */
    public FileSystem getFileSystem() {
        LOG.info("Destory fileSystem");
        return fileSystem;
    }
}
