
package de.hhu.bsinfo.dxram.log;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.header.AbstractLogEntryHeader;
import de.hhu.bsinfo.dxram.log.header.DefaultPrimLogEntryHeader;
import de.hhu.bsinfo.dxram.log.header.MigrationPrimLogEntryHeader;
import de.hhu.bsinfo.dxram.log.messages.GetUtilizationRequest;
import de.hhu.bsinfo.dxram.log.messages.GetUtilizationResponse;
import de.hhu.bsinfo.dxram.log.messages.InitRequest;
import de.hhu.bsinfo.dxram.log.messages.InitResponse;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.log.messages.LogMessages;
import de.hhu.bsinfo.dxram.log.messages.RemoveMessage;
import de.hhu.bsinfo.dxram.log.storage.LogCatalog;
import de.hhu.bsinfo.dxram.log.storage.PrimaryLog;
import de.hhu.bsinfo.dxram.log.storage.PrimaryWriteBuffer;
import de.hhu.bsinfo.dxram.log.storage.SecondaryLog;
import de.hhu.bsinfo.dxram.log.storage.SecondaryLogBuffer;
import de.hhu.bsinfo.dxram.log.storage.Version;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.utils.Tools;
import de.hhu.bsinfo.utils.unit.StorageUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This service provides access to the backend storage system.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class LogService extends AbstractDXRAMService implements MessageReceiver {

	private static final Logger LOGGER = LogManager.getFormatterLogger(LogService.class.getSimpleName());

	// Constants
	private static final AbstractLogEntryHeader DEFAULT_PRIM_LOG_ENTRY_HEADER = new DefaultPrimLogEntryHeader();
	private static final AbstractLogEntryHeader MIGRATION_PRIM_LOG_ENTRY_HEADER = new MigrationPrimLogEntryHeader();
	private static final int PAYLOAD_PRINT_LENGTH = 128;

	// configuration values
	@Expose
	private boolean m_useChecksum = true;
	@Expose
	private StorageUnit m_flashPageSize = new StorageUnit(4, StorageUnit.KB);
	@Expose
	private StorageUnit m_logSegmentSize = new StorageUnit(8, StorageUnit.MB);
	@Expose
	private StorageUnit m_primaryLogSize = new StorageUnit(256, StorageUnit.MB);
	@Expose
	private StorageUnit m_secondaryLogSize = new StorageUnit(512, StorageUnit.MB);
	@Expose
	private StorageUnit m_writeBufferSize = new StorageUnit(256, StorageUnit.MB);
	@Expose
	private StorageUnit m_secondaryLogBufferSize = new StorageUnit(128, StorageUnit.KB);
	@Expose
	private int m_reorgUtilizationThreshold = 70;
	@Expose
	private boolean m_sortBufferPooling = true;

	// dependent components
	private NetworkComponent m_network;
	private AbstractBootComponent m_boot;
	private LogComponent m_log;
	private BackupComponent m_backup;

	// private state
	private short m_nodeID;
	private boolean m_loggingIsActive;

	private PrimaryWriteBuffer m_writeBuffer;
	private PrimaryLog m_primaryLog;
	private LogCatalog[] m_logCatalogs;

	private ReentrantReadWriteLock m_secondaryLogCreationLock;

	private SecondaryLogsReorgThread m_secondaryLogsReorgThread;

	private ReentrantLock m_flushLock;

	private boolean m_reorgThreadWaits;
	private boolean m_accessGrantedForReorgThread;

	private String m_backupDirectory;

	/**
	 * Constructor
	 */
	public LogService() {
		super("log");
	}

	/**
	 * Returns all log catalogs
	 *
	 * @return the array of log catalogs
	 */
	public LogCatalog[] getAllLogCatalogs() {
		return m_logCatalogs;
	}

	/**
	 * Returns the secondary log
	 *
	 * @param p_chunkID the ChunkID
	 * @param p_source  the source NodeID
	 * @param p_rangeID the RangeID for migrations or -1
	 * @return the secondary log
	 * @throws IOException          if the secondary log could not be returned
	 * @throws InterruptedException if the secondary log could not be returned
	 */
	public SecondaryLog getSecondaryLog(final long p_chunkID, final short p_source,
			final byte p_rangeID) throws IOException, InterruptedException {
		SecondaryLog ret;
		LogCatalog cat;

		// Can be executed by application/network thread or writer thread
		m_secondaryLogCreationLock.readLock().lock();
		if (p_rangeID == -1) {
			cat = m_logCatalogs[ChunkID.getCreatorID(p_chunkID) & 0xFFFF];
		} else {
			cat = m_logCatalogs[p_source & 0xFFFF];
		}
		ret = cat.getLog(p_chunkID, p_rangeID);
		m_secondaryLogCreationLock.readLock().unlock();

		return ret;
	}

	/**
	 * Returns the secondary log buffer
	 *
	 * @param p_chunkID the ChunkID
	 * @param p_source  the source NodeID
	 * @param p_rangeID the RangeID for migrations or -1
	 * @return the secondary log buffer
	 * @throws IOException          if the secondary log buffer could not be returned
	 * @throws InterruptedException if the secondary log buffer could not be returned
	 */
	public SecondaryLogBuffer getSecondaryLogBuffer(final long p_chunkID, final short p_source, final byte p_rangeID)
			throws IOException,
			InterruptedException {
		SecondaryLogBuffer ret = null;
		LogCatalog cat;

		// Can be executed by application/network thread or writer thread
		m_secondaryLogCreationLock.readLock().lock();
		if (p_rangeID == -1) {
			cat = m_logCatalogs[ChunkID.getCreatorID(p_chunkID) & 0xFFFF];
		} else {
			cat = m_logCatalogs[p_source & 0xFFFF];
		}

		if (cat != null) {
			ret = cat.getBuffer(p_chunkID, p_rangeID);
		}
		m_secondaryLogCreationLock.readLock().unlock();

		return ret;
	}

	/**
	 * Returns the backup range
	 *
	 * @param p_chunkID the ChunkID
	 * @return the first ChunkID of the range
	 */
	public long getBackupRange(final long p_chunkID) {
		long ret = -1;
		LogCatalog cat;

		// Can be executed by application/network thread or writer thread
		m_secondaryLogCreationLock.readLock().lock();

		cat = m_logCatalogs[ChunkID.getCreatorID(p_chunkID) & 0xFFFF];
		ret = cat.getRange(p_chunkID);
		m_secondaryLogCreationLock.readLock().unlock();

		return (p_chunkID & 0xFFFF000000000000L) + ret;
	}

	/**
	 * Prints the metadata of one node's log
	 *
	 * @param p_owner   the NodeID
	 * @param p_chunkID the ChunkID
	 * @param p_rangeID the RangeID
	 * @note for testing only
	 */
	public void printBackupRange(final short p_owner, final long p_chunkID, final byte p_rangeID) {
		byte[][] segments;
		int i = 0;
		int j = 1;
		int readBytes;
		int length;
		int offset = 0;
		long chunkID;
		Version version;
		AbstractLogEntryHeader logEntryHeader;

		segments = readBackupRange(p_owner, p_chunkID, p_rangeID);
		if (segments != null) {
			System.out.println();
			System.out.println("NodeID: " + p_owner);
			while (segments[i] != null) {
				System.out.println("Segment " + i + ": " + segments[i].length);
				readBytes = offset;
				offset = 0;
				while (readBytes < segments[i].length) {
					logEntryHeader = AbstractLogEntryHeader.getSecondaryHeader(segments[i], readBytes,
							p_owner != ChunkID.getCreatorID(p_chunkID));
					chunkID = logEntryHeader.getCID(segments[i], readBytes);
					length = logEntryHeader.getLength(segments[i], readBytes);
					version = logEntryHeader.getVersion(segments[i], readBytes);
					printMetadata(ChunkID.getCreatorID(p_chunkID), chunkID, segments[i], readBytes, length, version,
							j++, logEntryHeader);
					readBytes += length + logEntryHeader.getHeaderSize(segments[i], readBytes);
				}
				i++;
			}
		}
	}

	/**
	 * Grant the writer thread access to write buffer
	 */
	public void grantAccessToWriterThread() {
		m_writeBuffer.grantAccessToWriterThread();
	}

	/**
	 * Grants the reorganization thread access to a secondary log
	 */
	public void grantReorgThreadAccessToCurrentLog() {
		if (m_reorgThreadWaits) {
			m_accessGrantedForReorgThread = true;
		}
	}

	/**
	 * Flushes the primary log write buffer
	 *
	 * @throws IOException          if primary log could not be flushed
	 * @throws InterruptedException if caller is interrupted
	 */
	public void flushDataToPrimaryLog() throws IOException, InterruptedException {
		m_writeBuffer.signalWriterThreadAndFlushToPrimLog();
	}

	/**
	 * Flushes all secondary log buffers
	 *
	 * @throws IOException          if at least one secondary log could not be flushed
	 * @throws InterruptedException if caller is interrupted
	 */
	public void flushDataToSecondaryLogs() throws IOException, InterruptedException {
		LogCatalog cat;
		SecondaryLogBuffer[] buffers;

		if (m_flushLock.tryLock()) {
			try {
				for (int i = 0; i < Short.MAX_VALUE * 2 + 1; i++) {
					cat = m_logCatalogs[i];
					if (cat != null) {
						buffers = cat.getAllBuffers();
						for (int j = 0; j < buffers.length; j++) {
							if (buffers[j] != null && !buffers[j].isBufferEmpty()) {
								buffers[j].flushSecLogBuffer();
							}
						}
					}
				}
			} finally {
				m_flushLock.unlock();
			}
		} else {
			// Another thread is flushing, wait until it is finished
			do {
				Thread.sleep(100);
			} while (m_flushLock.isLocked());
		}
	}

	@Override
	protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
		m_network = p_componentAccessor.getComponent(NetworkComponent.class);
		m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
		m_log = p_componentAccessor.getComponent(LogComponent.class);
		m_backup = p_componentAccessor.getComponent(BackupComponent.class);
	}

	@Override
	protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {

		registerNetworkMessages();
		registerNetworkMessageListener();

		m_loggingIsActive = (m_boot.getNodeRole() == NodeRole.PEER) && m_backup.isActive();
		if (m_loggingIsActive) {

			m_nodeID = m_boot.getNodeID();

			m_backupDirectory = m_backup.getBackupDirectory();

			// Set attributes of log component that can only be set by log service
			m_log.setAttributes(this, m_backupDirectory, m_useChecksum,
					m_secondaryLogSize.getBytes(), (int) m_logSegmentSize.getBytes());

			// Set the log entry header crc size (must be called before the first log entry header is created)
			AbstractLogEntryHeader.setCRCSize(m_useChecksum);

			// Create primary log
			try {
				m_primaryLog = new PrimaryLog(this, m_backupDirectory, m_nodeID, m_primaryLogSize.getBytes(),
						(int) m_flashPageSize.getBytes());
			} catch (final IOException | InterruptedException e) {
				// #if LOGGER >= ERROR
				LOGGER.error("Primary log creation failed", e);
				// #endif /* LOGGER >= ERROR */
			}
			// #if LOGGER == TRACE
			LOGGER.trace("Initialized primary log (%d)", m_primaryLogSize);
			// #endif /* LOGGER == TRACE */

			// Create primary log buffer
			m_writeBuffer = new PrimaryWriteBuffer(this, m_primaryLog, (int) m_writeBufferSize.getBytes(),
					(int) m_flashPageSize.getBytes(), (int) m_secondaryLogBufferSize.getBytes(),
					(int) m_logSegmentSize.getBytes(), m_useChecksum, m_sortBufferPooling);

			// Create secondary log and secondary log buffer catalogs
			m_logCatalogs = new LogCatalog[Short.MAX_VALUE * 2 + 1];

			m_secondaryLogCreationLock = new ReentrantReadWriteLock(false);

			// Create reorganization thread for secondary logs
			m_secondaryLogsReorgThread =
					new SecondaryLogsReorgThread(this, m_secondaryLogSize.getBytes(),
							(int) m_logSegmentSize.getBytes());
			m_secondaryLogsReorgThread.setName("Logging: Reorganization Thread");
			// Start secondary logs reorganization thread
			m_secondaryLogsReorgThread.start();

			m_flushLock = new ReentrantLock(false);
		}

		return true;
	}

	@Override
	protected boolean shutdownService() {
		LogCatalog cat;

		if (m_loggingIsActive) {
			// Stop reorganization thread
			m_secondaryLogsReorgThread.interrupt();
			m_secondaryLogsReorgThread.shutdown();
			try {
				m_secondaryLogsReorgThread.join();
				// #if LOGGER >= INFO
				LOGGER.info("Shutdown of SecondaryLogsReorgThread successful");
				// #endif /* LOGGER >= INFO */
			} catch (final InterruptedException e1) {
				// #if LOGGER >= WARN
				LOGGER.warn("Could not wait for reorganization thread to finish. Interrupted");
				// #endif /* LOGGER >= WARN */
			}
			m_secondaryLogsReorgThread = null;

			// Close write buffer
			try {
				m_writeBuffer.closeWriteBuffer();
			} catch (final IOException | InterruptedException e) {
				// #if LOGGER >= WARN
				LOGGER.warn("Could not close write buffer!");
				// #endif /* LOGGER >= WARN */
			}
			m_writeBuffer = null;

			// Close primary log
			if (m_primaryLog != null) {
				try {
					m_primaryLog.closeLog();
				} catch (final IOException e) {
					// #if LOGGER >= WARN
					LOGGER.warn("Could not close primary log!");
					// #endif /* LOGGER >= WARN */
				}
				m_primaryLog = null;
			}

			// Clear secondary logs and buffers
			for (int i = 0; i < Short.MAX_VALUE * 2 + 1; i++) {
				try {
					cat = m_logCatalogs[i];
					if (cat != null) {
						cat.closeLogsAndBuffers();
					}
				} catch (final IOException | InterruptedException e) {
					// #if LOGGER >= WARN
					LOGGER.warn("Could not close secondary log buffer %d", i);
					// #endif /* LOGGER >= WARN */
				}
			}
			m_logCatalogs = null;
		}

		return true;
	}

	/**
	 * Get access to secondary log for reorganization thread
	 *
	 * @param p_secLog the Secondary Log
	 */
	protected void getAccessToSecLog(final SecondaryLog p_secLog) {
		if (!p_secLog.isAccessed()) {
			p_secLog.setAccessFlag(true);

			while (!m_accessGrantedForReorgThread) {
				m_reorgThreadWaits = true;
				Thread.yield();
			}
			m_accessGrantedForReorgThread = false;
		}
	}

	/**
	 * Get access to secondary log for reorganization thread
	 *
	 * @param p_secLog the Secondary Log
	 */
	protected void leaveSecLog(final SecondaryLog p_secLog) {
		if (p_secLog.isAccessed()) {
			p_secLog.setAccessFlag(false);
		}
	}

	/**
	 * Prints the metadata of one log entry
	 *
	 * @param p_nodeID         the NodeID
	 * @param p_localID        the LocalID
	 * @param p_payload        buffer with payload
	 * @param p_offset         offset within buffer
	 * @param p_length         length of payload
	 * @param p_version        version of chunk
	 * @param p_index          index of log entry
	 * @param p_logEntryHeader the log entry header
	 */
	private void printMetadata(final short p_nodeID, final long p_localID, final byte[] p_payload, final int p_offset,
			final int p_length,
			final Version p_version, final int p_index, final AbstractLogEntryHeader p_logEntryHeader) {
		final long chunkID = ((long) p_nodeID << 48) + p_localID;
		byte[] array;

		try {
			if (p_version.getVersion() != -1) {
				array =
						new String(Arrays.copyOfRange(p_payload,
								p_offset + p_logEntryHeader.getHeaderSize(p_payload, p_offset), p_offset
										+ p_logEntryHeader.getHeaderSize(p_payload, p_offset) + PAYLOAD_PRINT_LENGTH))
								.trim().getBytes();

				if (Tools.looksLikeUTF8(array)) {
					System.out.println("Log Entry " + p_index + ": \t ChunkID - " + chunkID + "(" + p_nodeID + ", "
							+ (int) p_localID + ") \t Length - "
							+ p_length + "\t Version - " + p_version.getEpoch() + "," + p_version.getVersion()
							+ " \t Payload - " + new String(array, "UTF-8"));
				} else {
					System.out.println("Log Entry " + p_index + ": \t ChunkID - " + chunkID + "(" + p_nodeID + ", "
							+ (int) p_localID + ") \t Length - "
							+ p_length + "\t Version - " + p_version.getEpoch() + "," + p_version.getVersion()
							+ " \t Payload is no String");
				}
			} else {
				System.out.println("Log Entry " + p_index + ": \t ChunkID - " + chunkID + "(" + p_nodeID + ", "
						+ (int) p_localID + ") \t Length - " + p_length
						+ "\t Version - " + p_version.getEpoch() + "," + p_version.getVersion()
						+ " \t Tombstones have no payload");
			}
		} catch (final UnsupportedEncodingException | IllegalArgumentException e) {
			System.out.println("Log Entry " + p_index + ": \t ChunkID - " + chunkID + "(" + p_nodeID + ", "
					+ (int) p_localID + ") \t Length - " + p_length
					+ "\t Version - " + p_version.getEpoch() + "," + p_version.getVersion()
					+ " \t Payload is no String");
		}
		// p_localID: -1 can only be printed as an int
	}

	/**
	 * Reads the local data of one log
	 *
	 * @param p_owner   the NodeID
	 * @param p_chunkID the ChunkID
	 * @param p_rangeID the RangeID
	 * @return the local data
	 * @note for testing only
	 */
	private byte[][] readBackupRange(final short p_owner, final long p_chunkID, final byte p_rangeID) {
		byte[][] ret = null;
		SecondaryLogBuffer secondaryLogBuffer;

		try {
			flushDataToPrimaryLog();
			flushDataToSecondaryLogs();

			secondaryLogBuffer = getSecondaryLogBuffer(p_chunkID, p_owner, p_rangeID);
			if (secondaryLogBuffer != null) {
				secondaryLogBuffer.flushSecLogBuffer();

				ret = getSecondaryLog(p_chunkID, p_owner, p_rangeID).readAllSegments();
			}
		} catch (final IOException | InterruptedException e) {
		}

		return ret;
	}

	/**
	 * Returns the current utilization of primary log and all secondary logs
	 *
	 * @return the current utilization
	 */
	public String getCurrentUtilization() {
		String ret;
		long allBytes = 0;
		long counter;
		SecondaryLog[] secondaryLogs;
		SecondaryLogBuffer[] secLogBuffers;
		LogCatalog cat;

		ret = "***********************************************************************\n"
				+ "*Primary log: " + m_primaryLog.getOccupiedSpace() + " bytes\n"
				+ "***********************************************************************\n\n"
				+ "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
				+ "+Secondary logs:\n";

		for (int i = 0; i < m_logCatalogs.length; i++) {
			cat = m_logCatalogs[i];
			if (cat != null) {
				counter = 0;
				ret += "++Node " + (short) i + ":\n";
				secondaryLogs = cat.getAllCreatorLogs();
				secLogBuffers = cat.getAllCreatorBuffers();
				for (int j = 0; j < secondaryLogs.length; j++) {
					ret += "+++Creator backup range " + j + ": ";
					if (secondaryLogs[j] != null) {
						if (secondaryLogs[j].isAccessed()) {
							ret += "#Active log# ";
						}
						ret += secondaryLogs[j].getOccupiedSpace() + " bytes (in buffer: "
								+ secLogBuffers[j].getOccupiedSpace() + " bytes)\n";
						ret += secondaryLogs[j].getSegmentDistribution() + "\n";
						counter += secondaryLogs[j].getLogFileSize() + secondaryLogs[j].getVersionsFileSize();
					}
				}
				secondaryLogs = cat.getAllMigrationLogs();
				secLogBuffers = cat.getAllMigrationBuffers();
				for (int j = 0; j < secondaryLogs.length; j++) {
					ret += "+++Migration backup range " + j + ": ";
					if (secondaryLogs[j] != null) {
						if (secondaryLogs[j].isAccessed()) {
							ret += "#Active log# ";
						}
						ret += secondaryLogs[j].getOccupiedSpace() + " bytes (in buffer: "
								+ secLogBuffers[j].getOccupiedSpace() + " bytes)\n";
						ret += secondaryLogs[j].getSegmentDistribution() + "\n";
						counter += secondaryLogs[j].getLogFileSize() + secondaryLogs[j].getVersionsFileSize();
					}
				}
				ret += "++Bytes per node: " + counter + "\n";
				allBytes += counter;
			}
		}
		ret += "Complete size: " + allBytes + "\n";
		ret += "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";

		return ret;
	}

	/**
	 * Returns the current utilization of given node
	 *
	 * @param p_nodeID the NodeID of the peer whose utilization is printed
	 * @return the current utilization
	 */
	public String getCurrentUtilization(final short p_nodeID) {
		final GetUtilizationRequest request = new GetUtilizationRequest(p_nodeID);

		try {
			m_network.sendSync(request);
		} catch (final NetworkException e) {
			// #if LOGGER >= ERROR
			LOGGER.error("Could not get log utilization of " + p_nodeID);
			// #endif /* LOGGER >= ERROR */
			return "";
		}

		return ((GetUtilizationResponse) request.getResponse()).getUtilization();
	}

	/**
	 * Handles an incoming LogMessage
	 *
	 * @param p_message the LogMessage
	 */
	private void incomingLogMessage(final LogMessage p_message) {
		long chunkID;
		int length;
		byte[] logEntryHeader;
		final ByteBuffer buffer = p_message.getMessageBuffer();
		final short source = p_message.getSource();
		final byte rangeID = buffer.get();
		final int size = buffer.getInt();
		SecondaryLog secLog;

		for (int i = 0; i < size; i++) {
			chunkID = buffer.getLong();
			length = buffer.getInt();

			assert length > 0;

			try {
				secLog = getSecondaryLog(chunkID, source, rangeID);
				if (rangeID == -1) {
					logEntryHeader = DEFAULT_PRIM_LOG_ENTRY_HEADER.createLogEntryHeader(chunkID, length,
							secLog.getNextVersion(chunkID), (byte) -1, (short) -1);
				} else {
					logEntryHeader = MIGRATION_PRIM_LOG_ENTRY_HEADER.createLogEntryHeader(chunkID, length,
							secLog.getNextVersion(chunkID), rangeID, source);
				}

				m_writeBuffer.putLogData(logEntryHeader, buffer, length);
			} catch (final IOException | InterruptedException e) {
				// #if LOGGER >= ERROR
				LOGGER.error("Logging of chunk 0x%X failed: %s", chunkID, e);
				// #endif /* LOGGER >= ERROR */
			}
		}
	}

	/**
	 * Handles an incoming RemoveMessage
	 *
	 * @param p_message the RemoveMessage
	 */
	private void incomingRemoveMessage(final RemoveMessage p_message) {
		long chunkID;
		final ByteBuffer buffer = p_message.getMessageBuffer();
		final short source = p_message.getSource();
		final byte rangeID = buffer.get();
		final int size = buffer.getInt();

		for (int i = 0; i < size; i++) {
			chunkID = buffer.getLong();

			try {
				getSecondaryLog(chunkID, source, rangeID).invalidateChunk(chunkID);
			} catch (final IOException | InterruptedException e) {
				// #if LOGGER >= ERROR
				LOGGER.error("Deletion of chunk 0x%X failed: %s", chunkID, e);
				// #endif /* LOGGER >= ERROR */
			}
		}
	}

	/**
	 * Handles an incoming InitRequest
	 *
	 * @param p_request the InitRequest
	 */
	private void incomingInitRequest(final InitRequest p_request) {
		short owner;
		long firstChunkIDOrRangeID;
		boolean success = true;
		LogCatalog cat;
		SecondaryLog secLog = null;

		owner = p_request.getOwner();
		firstChunkIDOrRangeID = p_request.getFirstCIDOrRangeID();

		m_secondaryLogCreationLock.writeLock().lock();
		cat = m_logCatalogs[owner & 0xFFFF];
		if (cat == null) {
			cat = new LogCatalog();
			m_logCatalogs[owner & 0xFFFF] = cat;
		}
		try {
			if (owner == ChunkID.getCreatorID(firstChunkIDOrRangeID)) {
				if (!cat.exists(firstChunkIDOrRangeID, (byte) -1)) {
					// Create new secondary log
					secLog =
							new SecondaryLog(this, m_secondaryLogsReorgThread, owner,
									ChunkID.getLocalID(firstChunkIDOrRangeID), cat.getNewID(false),
									false,
									m_backupDirectory, m_secondaryLogSize.getBytes(),
									(int) m_flashPageSize.getBytes(), (int) m_logSegmentSize.getBytes(),
									m_reorgUtilizationThreshold, m_useChecksum);
					// Insert range in log catalog
					cat.insertRange(firstChunkIDOrRangeID, secLog, (int) m_secondaryLogBufferSize.getBytes(),
							(int) m_logSegmentSize.getBytes());
				}
			} else {
				if (!cat.exists(-1, (byte) firstChunkIDOrRangeID)) {
					// Create new secondary log for migrations
					secLog = new SecondaryLog(this, m_secondaryLogsReorgThread, owner, firstChunkIDOrRangeID,
							cat.getNewID(true), true,
							m_backupDirectory, m_secondaryLogSize.getBytes(), (int) m_flashPageSize.getBytes(),
							(int) m_logSegmentSize.getBytes(),
							m_reorgUtilizationThreshold, m_useChecksum);
					// Insert range in log catalog
					cat.insertRange(firstChunkIDOrRangeID, secLog, (int) m_secondaryLogBufferSize.getBytes(),
							(int) m_logSegmentSize.getBytes());
				}
			}
		} catch (final IOException | InterruptedException e) {
			// #if LOGGER >= ERROR
			LOGGER.error("Initialization of backup range %d failed: %s", firstChunkIDOrRangeID, e);
			// #endif /* LOGGER >= ERROR */
			success = false;
		}
		m_secondaryLogCreationLock.writeLock().unlock();

		try {
			m_network.sendMessage(new InitResponse(p_request, success));
		} catch (final NetworkException e) {
			LOGGER.error("Could not acknowledge initilization of backup range", e);
		}
	}

	/**
	 * Handles an incoming GetUtilizationRequest
	 *
	 * @param p_request the GetUtilizationRequest
	 */
	private void incomingGetUtilizationRequest(final GetUtilizationRequest p_request) {

		try {
			if (m_loggingIsActive) {
				m_network.sendMessage(new GetUtilizationResponse(p_request, getCurrentUtilization()));
			} else {
				// #if LOGGER >= WARN
				LOGGER.warn("Incoming GetUtilizationRequest, but superpeers do not store backups");
				// #endif /* LOGGER >= WARN */

				m_network.sendMessage(new GetUtilizationResponse(p_request, null));
			}
		} catch (final NetworkException e) {
			// #if LOGGER >= ERROR
			LOGGER.error("Could not answer GetUtilizationRequest", e);
			// #endif /* LOGGER >= ERROR */
		}
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == DXRAMMessageTypes.LOG_MESSAGES_TYPE) {
				switch (p_message.getSubtype()) {
					case LogMessages.SUBTYPE_LOG_MESSAGE:
						incomingLogMessage((LogMessage) p_message);
						break;
					case LogMessages.SUBTYPE_REMOVE_MESSAGE:
						incomingRemoveMessage((RemoveMessage) p_message);
						break;
					case LogMessages.SUBTYPE_INIT_REQUEST:
						incomingInitRequest((InitRequest) p_message);
						break;
					case LogMessages.SUBTYPE_GET_UTILIZATION_REQUEST:
						incomingGetUtilizationRequest((GetUtilizationRequest) p_message);
						break;
					default:
						break;
				}
			}
		}
	}

	// -----------------------------------------------------------------------------------

	/**
	 * Register network messages we use in here.
	 */
	private void registerNetworkMessages() {
		m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_LOG_MESSAGE,
				LogMessage.class);
		m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_REMOVE_MESSAGE,
				RemoveMessage.class);
		m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_GET_UTILIZATION_REQUEST,
				GetUtilizationRequest.class);
		m_network.registerMessageType(DXRAMMessageTypes.LOG_MESSAGES_TYPE, LogMessages.SUBTYPE_GET_UTILIZATION_RESPONSE,
				GetUtilizationResponse.class);
	}

	/**
	 * Register network messages we want to listen to in here.
	 */
	private void registerNetworkMessageListener() {
		m_network.register(LogMessage.class, this);
		m_network.register(RemoveMessage.class, this);
		m_network.register(InitRequest.class, this);
		m_network.register(GetUtilizationRequest.class, this);
	}

}
