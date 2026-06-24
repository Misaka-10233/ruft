// 项目入口：当前二进制只加载模块，Raft 节点逻辑位于 ruft/rpc/events/utilis 中。
// Project entrypoint: this binary only wires modules; Raft logic lives in ruft/rpc/events/utilis.
// 项目入口：当前二进制只加载模块，Raft 节点逻辑位于 ruft/rpc/events/utilis 中。
// Project entrypoint: this binary only wires modules; Raft logic lives in ruft/rpc/events/utilis.
mod events;
mod rpc;
mod ruft;
mod utilis;

fn main() {
    println!("Hello, world!");
}
