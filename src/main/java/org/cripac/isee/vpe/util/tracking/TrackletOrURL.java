/*
 * This file is part of las-vpe-platform.
 *
 * las-vpe-platform is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * las-vpe-platform is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with las-vpe-platform. If not, see <http://www.gnu.org/licenses/>.
 *
 * Created by ken.yu on 17-2-27.
 */

package org.cripac.isee.vpe.util.tracking;

import org.apache.spark.api.java.function.Function;
import org.cripac.isee.pedestrian.tracking.Tracklet;
import org.cripac.isee.vpe.common.RobustExecutor;
import org.cripac.isee.vpe.util.hdfs.HadoopHelper;

import java.io.Serializable;

/**
 * A tracklet might be passed directly or first stored in the HDFS then passed by URL instead.
 * This class is an either option of these two methods.
 * When calling {@link TrackletOrURL#getTracklet()}, this class returns the tracklet it represents,
 * no matter it is stored in this class or in HDFS.
 */
public class TrackletOrURL implements Serializable {
    private static final long serialVersionUID = -1204135134550824273L;
    private Tracklet tracklet;

    public String getURL() {
        return URL;
    }

    public void setURL(String URL) {
        if (this.URL != null && !this.URL.equals(URL)) {
            tracklet = null;
        }
        this.URL = URL;
    }

    private String URL;

    public TrackletOrURL(String URL) {
        this(null, URL);
    }

    public TrackletOrURL(Tracklet tracklet) {
        this(tracklet, null);
    }

    public TrackletOrURL(Tracklet tracklet, String URL) {
        this.tracklet = tracklet;
        this.URL = URL;
    }

    public Tracklet getTracklet() throws Exception {
        return tracklet != null ? tracklet : (tracklet =
                new RobustExecutor<>((Function<String, Tracklet>) HadoopHelper::retrieveTracklet).execute(URL));
    }
}
