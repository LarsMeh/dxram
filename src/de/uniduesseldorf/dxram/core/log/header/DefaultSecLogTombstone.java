
package de.uniduesseldorf.dxram.core.log.header;

import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.log.LogHandler;

/**
 * Implements a log entry header for removal (secondary log)
 * @author Kevin Beineke
 *         25.06.2015
 */
public class DefaultSecLogTombstone implements LogEntryHeaderInterface {
	// Attributes
	public static final short SIZE = 10;
	public static final byte VER_OFFSET = LogHandler.LOG_ENTRY_LID_SIZE;

	// Constructors
	/**
	 * Creates an instance of TombstoneSecondaryLog
	 */
	public DefaultSecLogTombstone() {}

	// Methods
	@Override
	public byte[] createHeader(final Chunk p_chunk, final byte p_rangeID, final short p_source) {
		System.out.println("Do not call createHeader() for secondary log entries. Convert instead.");

		return null;
	}

	@Override
	public byte getRangeID(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		System.out.println("No RangeID available!");
		return -1;
	}

	@Override
	public short getSource(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		System.out.println("No source available!");
		return -1;
	}

	@Override
	public short getNodeID(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		short ret = -1;

		if (p_logStoresMigrations) {
			ret = (short) ((p_buffer[p_offset] & 0xff) + ((p_buffer[p_offset + 1] & 0xff) << 8));
		} else {
			System.out.println("No NodeID available!");
		}
		return ret;
	}

	@Override
	public long getLID(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		int offset = p_offset;

		if (p_logStoresMigrations) {
			offset += LogHandler.LOG_ENTRY_NID_SIZE;
		}

		return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
				+ ((p_buffer[offset + 2] & 0xff) << 16) + (((long) p_buffer[offset + 3] & 0xff) << 24)
				+ (((long) p_buffer[offset + 4] & 0xff) << 32) + (((long) p_buffer[offset + 5] & 0xff) << 40);
	}

	@Override
	public long getChunkID(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		long ret = -1;

		if (p_logStoresMigrations) {
			ret = ((long) getNodeID(p_buffer, p_offset, p_logStoresMigrations) << 48)
					+ getLID(p_buffer, p_offset, p_logStoresMigrations);
		} else {
			System.out.println("No ChunkID available!");
		}

		return ret;
	}

	@Override
	public int getLength(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		return 0;
	}

	@Override
	public int getVersion(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		int offset = p_offset + VER_OFFSET;

		if (p_logStoresMigrations) {
			offset += LogHandler.LOG_ENTRY_NID_SIZE;
		}

		return (p_buffer[offset] & 0xff) + ((p_buffer[offset + 1] & 0xff) << 8)
				+ ((p_buffer[offset + 2] & 0xff) << 16) + ((p_buffer[offset + 3] & 0xff) << 24);
	}

	@Override
	public long getChecksum(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		System.out.println("No checksum available!");
		return -1;
	}

	@Override
	public short getHeaderSize(final boolean p_logStoresMigrations) {
		short ret = SIZE;

		if (p_logStoresMigrations) {
			ret += LogHandler.LOG_ENTRY_NID_SIZE;
		}

		return ret;
	}

	@Override
	public short getConversionOffset(final boolean p_logStoresMigrations) {
		return getLIDOffset(p_logStoresMigrations);
	}

	@Override
	public short getRIDOffset(final boolean p_logStoresMigrations) {
		System.out.println("No RangeID available!");
		return -1;
	}

	@Override
	public short getSRCOffset(final boolean p_logStoresMigrations) {
		System.out.println("No source available!");
		return -1;
	}

	@Override
	public short getNIDOffset(final boolean p_logStoresMigrations) {
		short ret = -1;

		if (p_logStoresMigrations) {
			ret = 0;
		} else {
			System.out.println("No NodeID available!");
		}

		return ret;
	}

	@Override
	public short getLIDOffset(final boolean p_logStoresMigrations) {
		short ret = 0;

		if (p_logStoresMigrations) {
			ret += LogHandler.LOG_ENTRY_NID_SIZE;
		}

		return ret;
	}

	@Override
	public short getLENOffset(final boolean p_logStoresMigrations) {
		System.out.println("No length available!");
		return -1;
	}

	@Override
	public short getVEROffset(final boolean p_logStoresMigrations) {
		short ret = VER_OFFSET;

		if (p_logStoresMigrations) {
			ret += LogHandler.LOG_ENTRY_NID_SIZE;
		}

		return ret;
	}

	@Override
	public short getCRCOffset(final boolean p_logStoresMigrations) {
		System.out.println("No checksum available!");
		return -1;
	}

	@Override
	public void print(final byte[] p_buffer, final int p_offset, final boolean p_logStoresMigrations) {
		System.out.println("********************Tombstone for Secondary Log********************");
		System.out.println("* LocalID: " + getLID(p_buffer, p_offset, p_logStoresMigrations));
		System.out.println("* Length: " + getLength(p_buffer, p_offset, p_logStoresMigrations));
		System.out.println("* Version: " + getVersion(p_buffer, p_offset, p_logStoresMigrations));
		System.out.println("*******************************************************************");
	}
}
