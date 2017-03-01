/*
 * This file is part of LaS-VPE Platform.
 *
 * LaS-VPE Platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * LaS-VPE Platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with LaS-VPE Platform.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cripac.isee.vpe.util.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.cripac.isee.vpe.util.Factory;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Created by ken.yu on 16-10-26.
 */
public class HDFSFactory implements Factory<FileSystem> {
    /**
     * Produce a new Hadoop FileSystem instance.
     *
     * @return An object newly produced.
     * @throws Exception On failure creating a new instance.
     */
    @Nonnull
    @Override
    public FileSystem produce() throws IOException {
        Configuration hdfsConf = new Configuration();
        hdfsConf.addResource(new Path(System.getenv("HADOOP_HOME") + "/etc/hadoop/core-site.xml"));
        hdfsConf.setBoolean("dfs.support.append", true);
        hdfsConf.set("fs.hdfs.impl", 
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
                    );
            hdfsConf.set("fs.file.impl",
                    org.apache.hadoop.fs.LocalFileSystem.class.getName()
                        );
        System.out.println("Connecting to " + hdfsConf.getRaw("fs.default.name"));
        return FileSystem.get(hdfsConf);
    }
}
