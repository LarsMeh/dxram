
package de.hhu.bsinfo.dxram.log;

import java.io.IOException;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.header.AbstractLogEntryHeader;
import de.hhu.bsinfo.dxram.log.messages.InitRequest;
import de.hhu.bsinfo.dxram.log.messages.InitResponse;
import de.hhu.bsinfo.dxram.log.storage.SecondaryLog;
import de.hhu.bsinfo.dxram.log.storage.SecondaryLogBuffer;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.ethnet.NetworkException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Component for remote logging of chunks.
 *
 * @author Kevin Beineke <kevin.beineke@hhu.de> 30.03.16
 */
public class LogComponent extends AbstractDXRAMComponent {

	private static final Logger LOGGER = LogManager.getFormatterLogger(LogComponent.class.getSimpleName());

	// dependent components
	private AbstractBootComponent m_boot;
	private NetworkComponent m_network;

	private LogService m_logService;
	private boolean m_useChecksum;
	private int m_logSegmentSize;
	private long m_secondaryLogSize;

	/**
	 * Creates the log component
	 */
	public LogComponent() {
		super(DXRAMComponentOrder.Init.LOG, DXRAMComponentOrder.Shutdown.LOG);
	}

	/**
	 * Returns the header size
	 *
	 * @param p_nodeID  the NodeID
	 * @param p_localID the LocalID
	 * @param p_size    the size of the Chunk
	 * @return the header size
	 */
	public short getAproxHeaderSize(final short p_nodeID, final long p_localID, final int p_size) {
		return AbstractLogEntryHeader.getAproxSecLogHeaderSize(m_boot.getNodeID() != p_nodeID, p_localID, p_size);
	}

	/**
	 * Initializes a new backup range
	 *
	 * @param p_firstChunkIDOrRangeID the beginning of the range
	 * @param p_backupPeers           the backup peers
	 */
	public void initBackupRange(final long p_firstChunkIDOrRangeID, final short[] p_backupPeers) {
		InitRequest request;
		InitResponse response;
		long time;

		time = System.currentTimeMillis();
		if (null != p_backupPeers) {
			for (int i = 0; i < p_backupPeers.length; i++) {
				if (ChunkID.getCreatorID(p_firstChunkIDOrRangeID) != -1) {
					request = new InitRequest(p_backupPeers[i], p_firstChunkIDOrRangeID,
							ChunkID.getCreatorID(p_firstChunkIDOrRangeID));
				} else {
					request = new InitRequest(p_backupPeers[i], p_firstChunkIDOrRangeID, m_boot.getNodeID());
				}

				try {
					m_network.sendSync(request);
				} catch (final NetworkException e) {
					i--;
					continue;
				}

				response = request.getResponse(InitResponse.class);

				if (!response.getStatus()) {
					i--;
				}
			}
		}
		// #if LOGGER == TRACE
		LOGGER.trace("Time to initialize range: %d", (System.currentTimeMillis() - time));
		// #endif /* LOGGER == TRACE */
	}

	/**
	 * Recovers all Chunks of given backup range
	 *
	 * @param p_owner   the NodeID of the node whose Chunks have to be restored
	 * @param p_chunkID the ChunkID
	 * @param p_rangeID the RangeID
	 * @return the recovered Chunks
	 */
	public Chunk[] recoverBackupRange(final short p_owner, final long p_chunkID, final byte p_rangeID) {
		Chunk[] chunks = null;
		SecondaryLogBuffer secondaryLogBuffer;

		try {
			m_logService.flushDataToPrimaryLog();

			secondaryLogBuffer = m_logService.getSecondaryLogBuffer(p_chunkID, p_owner, p_rangeID);
			if (secondaryLogBuffer != null) {
				secondaryLogBuffer.flushSecLogBuffer();
				chunks = m_logService.getSecondaryLog(p_chunkID, p_owner, p_rangeID).recoverAllLogEntries(true);
			}
		} catch (final IOException | InterruptedException e) {
			// #if LOGGER >= ERROR
			LOGGER.error("Backup range recovery failed: %s", e);
			// #endif /* LOGGER >= ERROR */
		}

		return chunks;
	}

	/**
	 * Recovers all Chunks of given backup range
	 *
	 * @param p_fileName the file name
	 * @param p_path     the path of the folder the file is in
	 * @return the recovered Chunks
	 */
	public Chunk[] recoverBackupRangeFromFile(final String p_fileName, final String p_path) {
		Chunk[] ret = null;

		try {
			ret = SecondaryLog.recoverBackupRangeFromFile(p_fileName, p_path, m_useChecksum, m_secondaryLogSize,
					m_logSegmentSize);
		} catch (final IOException | InterruptedException e) {
			// #if LOGGER >= ERROR
			LOGGER.error("Could not recover from file %s: %s", p_path, e);
			// #endif /* LOGGER >= ERROR */
		}

		return ret;
	}

	@Override
	protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
		m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
		m_network = p_componentAccessor.getComponent(NetworkComponent.class);
	}

	@Override
	protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
		return true;
	}

	/**
	 * Sets attributes from log service
	 *
	 * @param p_logService       the log service
	 * @param p_backupDirectory  the backup directory
	 * @param p_useChecksum      whether checksums are used or not
	 * @param p_secondaryLogSize the secondary log size
	 * @param p_logSegmentSize   the segment size
	 */
	protected void setAttributes(final LogService p_logService, final String p_backupDirectory,
			final boolean p_useChecksum, final long p_secondaryLogSize, final int p_logSegmentSize) {
		m_logService = p_logService;
		m_useChecksum = p_useChecksum;
		m_secondaryLogSize = p_secondaryLogSize;
		m_logSegmentSize = p_logSegmentSize;
	}

	@Override
	protected boolean shutdownComponent() {
		return true;
	}

}
