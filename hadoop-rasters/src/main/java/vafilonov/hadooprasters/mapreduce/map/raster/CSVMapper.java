package vafilonov.hadooprasters.mapreduce.map.raster;

import org.apache.hadoop.io.Text;
import vafilonov.hadooprasters.mapreduce.map.AbstractGeodataMapper;

import vafilonov.hadooprasters.mapreduce.model.types.SentinelTile;
import vafilonov.hadooprasters.mapreduce.model.types.TilePosition;

public class CSVMapper extends AbstractGeodataMapper<TilePosition, SentinelTile, TilePosition, Text> {
}
