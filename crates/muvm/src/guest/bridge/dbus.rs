use std::collections::VecDeque;
use std::mem;
use std::os::fd::{AsFd, AsRawFd};

use anyhow::Result;
use log::{debug, warn};
use nix::errno::Errno;
use nix::ioctl_read;
use nix::sys::epoll::EpollFlags;
use nix::sys::socket::{socketpair, AddressFamily, SockFlag, SockType};

use crate::guest::bridge::common;
use crate::guest::bridge::common::{
    Client, CrossDomainHeader, CrossDomainResource, MessageResourceFinalizer, ProtocolHandler,
    StreamRecvResult, StreamSendResult,
};

#[repr(C)]
#[derive(Debug, Default)]
struct ExportedHandle {
    fs_id: u64,
    handle: u64,
}

const VIRTIO_IOC_MAGIC: u8 = b'v';
const VIRTIO_IOC_TYPE_EXPORT_FD: u8 = 1;

ioctl_read!(
    virtio_export_handle,
    VIRTIO_IOC_MAGIC,
    VIRTIO_IOC_TYPE_EXPORT_FD,
    ExportedHandle
);

#[repr(C)]
struct CrossDomainImportVirtioFsHandle {
    hdr: CrossDomainHeader,
    fs_id: u64,
    handle: u64,
    id: u32,
    pad: u32,
}

#[repr(C)]
pub struct CrossDomainAssignSocketUuid {
    hdr: CrossDomainHeader,
    token: [u8; 16],
    id: u32,
    pad: u32,
}

pub const CROSS_DOMAIN_CHANNEL_TYPE_DBUS_SESSION: u32 = 0x0012;

pub struct DBusResourceFinalizer;
impl MessageResourceFinalizer for DBusResourceFinalizer {
    type Handler = DBusProtocolHandler;

    fn finalize(self, _: &mut Client<Self::Handler>) -> Result<()> {
        unreachable!()
    }
}

pub const CROSS_DOMAIN_CMD_IMPORT_VIRTIOFS_HANDLE: u8 = 12;
pub const CROSS_DOMAIN_CMD_ASSIGN_SOCKET_UUID: u8 = 13;

pub const CROSS_DOMAIN_ID_TYPE_VIRTIO_FS_BLOB: u32 = 6;

pub struct DBusProtocolHandler {
    next_id: u32,
}
impl ProtocolHandler for DBusProtocolHandler {
    type ResourceFinalizer = DBusResourceFinalizer;

    const CHANNEL_TYPE: u32 = CROSS_DOMAIN_CHANNEL_TYPE_DBUS_SESSION;

    fn new() -> DBusProtocolHandler {
        DBusProtocolHandler { next_id: 0 }
    }

    fn process_recv_stream(
        this: &mut Client<Self>,
        data: &[u8],
        resources: &mut VecDeque<CrossDomainResource>,
    ) -> Result<StreamRecvResult> {
        let mut fds = Vec::new();
        for ident in resources.drain(..) {
            match ident.identifier_type {
                common::CROSS_DOMAIN_ID_TYPE_VIRTGPU_BLOB => {
                    fds.push(this.virtgpu_id_to_prime(ident)?)
                },
                common::CROSS_DOMAIN_ID_TYPE_SOCKET => {
                    let mut token = [0u8; 16];
                    getrandom::fill(&mut token)?;
                    let cmd = CrossDomainAssignSocketUuid {
                        hdr: CrossDomainHeader::new(
                            CROSS_DOMAIN_CMD_ASSIGN_SOCKET_UUID,
                            mem::size_of::<CrossDomainAssignSocketUuid>() as u16,
                        ),
                        token,
                        id: ident.identifier,
                        pad: 0,
                    };
                    this.gpu_ctx.submit_cmd(
                        &cmd,
                        mem::size_of::<CrossDomainAssignSocketUuid>(),
                        None,
                    )?;
                    let (client_half, proxy_half) = socketpair(
                        AddressFamily::Unix,
                        SockType::Stream,
                        None,
                        SockFlag::SOCK_CLOEXEC, // FdMapping will unset CLOEXEC when assigning the fd, but it won't close the others!
                    )?;
                    use command_fds::{CommandFdExt, FdMapping};
                    std::process::Command::new("/opt/bin/muvm-pwbridge")
                        .fd_mappings(vec![FdMapping {
                            parent_fd: proxy_half,
                            child_fd: 3,
                        }])?
                        .env("MUVM_PWBRIDGE_CLIENT_FD", "3")
                        .env("MUVM_PWBRIDGE_SOCKET_TOKEN", hex::encode(token))
                        .spawn()?;
                    fds.push(client_half);
                },
                x => warn!("unsupported identifier type {} for dbus recv", x),
            };
        }
        Ok(StreamRecvResult::Processed {
            consumed_bytes: data.len(),
            fds,
        })
    }

    fn process_send_stream(
        this: &mut Client<Self>,
        buf: &mut [u8],
    ) -> Result<StreamSendResult<DBusResourceFinalizer>> {
        let mut resources = Vec::new();
        let finalizers = Vec::new();
        let mut fds = this.request_fds.drain(..);
        while let Some(fd) = fds.next() {
            // TODO: check fd type!
            // XXX: unconditionally reopen as a "real" fd because we can't do O_PATH yet
            let tgt = rustix::fs::readlink(
                format!("/proc/self/fd/{}", fd.as_fd().as_raw_fd()),
                Vec::new(),
            )?;
            let xfd =
                rustix::fs::open(tgt, rustix::fs::OFlags::empty(), rustix::fs::Mode::empty())?;
            let mut handle: ExportedHandle = Default::default();
            unsafe { virtio_export_handle(xfd.as_raw_fd(), &mut handle) }?;
            let file_new_msg_size = mem::size_of::<CrossDomainImportVirtioFsHandle>();
            let id = this.protocol_handler.next_id;
            this.protocol_handler.next_id += 1;
            let file_msg = CrossDomainImportVirtioFsHandle {
                hdr: CrossDomainHeader::new(
                    CROSS_DOMAIN_CMD_IMPORT_VIRTIOFS_HANDLE,
                    file_new_msg_size as u16,
                ),
                id,
                fs_id: handle.fs_id,
                handle: handle.handle,
                pad: 0,
            };
            this.gpu_ctx
                .submit_cmd(&file_msg, file_new_msg_size, None)?;
            resources.push(CrossDomainResource {
                identifier: id,
                identifier_type: CROSS_DOMAIN_ID_TYPE_VIRTIO_FS_BLOB,
                identifier_size: 0,
            });
        }
        Ok(StreamSendResult::Processed {
            finalizers,
            resources,
            consumed_bytes: buf.len(),
        })
    }

    fn process_vgpu_extra(_this: &mut Client<Self>, _cmd: u8) -> Result<()> {
        return Err(Errno::EINVAL.into());
    }

    fn process_fd_extra(_: &mut Client<Self>, _: u64, _: EpollFlags) -> Result<()> {
        unreachable!()
    }
}
