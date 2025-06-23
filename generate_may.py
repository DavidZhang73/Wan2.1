import argparse
import logging
import os
import random
import socket
import sys
import warnings
from datetime import datetime

import nanoid
import torch
import torch.distributed as dist
from celery.signals import worker_shutting_down
from PIL import Image

import wan
from may import MayTask
from wan.configs import MAX_AREA_CONFIGS, SIZE_CONFIGS, SUPPORTED_SIZES, WAN_CONFIGS
from wan.utils.prompt_extend import DashScopePromptExpander, QwenPromptExpander
from wan.utils.utils import cache_image, cache_video, str2bool

worker_shutting_down.connect(lambda *args, **kwargs: dist.destroy_process_group())
warnings.filterwarnings("ignore")

EXAMPLE_PROMPT = {
    "t2v-1.3B": {
        "prompt": "Two anthropomorphic cats in comfy boxing gear and bright gloves fight intensely on a spotlighted stage.",
    },
    "t2v-14B": {
        "prompt": "Two anthropomorphic cats in comfy boxing gear and bright gloves fight intensely on a spotlighted stage.",
    },
    "t2i-14B": {
        "prompt": "一个朴素端庄的美人",
    },
    "i2v-14B": {
        "prompt": "Summer beach vacation style, a white cat wearing sunglasses sits on a surfboard. The fluffy-furred feline gazes directly at the camera with a relaxed expression. Blurred beach scenery forms the background featuring crystal-clear waters, distant green hills, and a blue sky dotted with white clouds. The cat assumes a naturally relaxed posture, as if savoring the sea breeze and warm sunlight. A close-up shot highlights the feline's intricate details and the refreshing atmosphere of the seaside.",
        "image": "examples/i2v_input.JPG",
    },
    "flf2v-14B": {
        "prompt": "CG动画风格，一只蓝色的小鸟从地面起飞，煽动翅膀。小鸟羽毛细腻，胸前有独特的花纹，背景是蓝天白云，阳光明媚。镜跟随小鸟向上移动，展现出小鸟飞翔的姿态和天空的广阔。近景，仰视视角。",
        "first_frame": "examples/flf2v_input_first_frame.png",
        "last_frame": "examples/flf2v_input_last_frame.png",
    },
    "vace-1.3B": {
        "src_ref_images": "examples/girl.png,examples/snake.png",
        "prompt": "在一个欢乐而充满节日气氛的场景中，穿着鲜艳红色春服的小女孩正与她的可爱卡通蛇嬉戏。她的春服上绣着金色吉祥图案，散发着喜庆的气息，脸上洋溢着灿烂的笑容。蛇身呈现出亮眼的绿色，形状圆润，宽大的眼睛让它显得既友善又幽默。小女孩欢快地用手轻轻抚摸着蛇的头部，共同享受着这温馨的时刻。周围五彩斑斓的灯笼和彩带装饰着环境，阳光透过洒在她们身上，营造出一个充满友爱与幸福的新年氛围。",
    },
    "vace-14B": {
        "src_ref_images": "examples/girl.png,examples/snake.png",
        "prompt": "在一个欢乐而充满节日气氛的场景中，穿着鲜艳红色春服的小女孩正与她的可爱卡通蛇嬉戏。她的春服上绣着金色吉祥图案，散发着喜庆的气息，脸上洋溢着灿烂的笑容。蛇身呈现出亮眼的绿色，形状圆润，宽大的眼睛让它显得既友善又幽默。小女孩欢快地用手轻轻抚摸着蛇的头部，共同享受着这温馨的时刻。周围五彩斑斓的灯笼和彩带装饰着环境，阳光透过洒在她们身上，营造出一个充满友爱与幸福的新年氛围。",
    },
}


def _validate_args(args):
    # Basic check
    assert args.ckpt_dir is not None, "Please specify the checkpoint directory."
    assert args.task in WAN_CONFIGS, f"Unsupport task: {args.task}"
    assert args.task in EXAMPLE_PROMPT, f"Unsupport task: {args.task}"

    # The default sampling steps are 40 for image-to-video tasks and 50 for text-to-video tasks.
    if args.sample_steps is None:
        args.sample_steps = 50
        if "i2v" in args.task:
            args.sample_steps = 40

    if args.sample_shift is None:
        args.sample_shift = 5.0
        if "i2v" in args.task and args.size in ["832*480", "480*832"]:
            args.sample_shift = 3.0
        elif "flf2v" in args.task or "vace" in args.task:
            args.sample_shift = 16

    # The default number of frames are 1 for text-to-image tasks and 81 for other tasks.
    if args.frame_num is None:
        args.frame_num = 1 if "t2i" in args.task else 81

    # T2I frame_num check
    if "t2i" in args.task:
        assert args.frame_num == 1, (
            f"Unsupport frame_num {args.frame_num} for task {args.task}"
        )

    args.base_seed = (
        args.base_seed if args.base_seed >= 0 else random.randint(0, sys.maxsize)
    )
    # Size check
    assert args.size in SUPPORTED_SIZES[args.task], (
        f"Unsupport size {args.size} for task {args.task}, supported sizes are: {', '.join(SUPPORTED_SIZES[args.task])}"
    )


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Generate a image or video from a text prompt or image using Wan"
    )
    parser.add_argument(
        "--task",
        type=str,
        # default="t2v-14B",
        default="t2v-1.3B",
        choices=list(WAN_CONFIGS.keys()),
        help="The task to run.",
    )
    parser.add_argument(
        "--size",
        type=str,
        # default="1280*720",
        default="480*832",
        choices=list(SIZE_CONFIGS.keys()),
        help="The area (width*height) of the generated video. For the I2V task, the aspect ratio of the output video will follow that of the input image.",
    )
    parser.add_argument(
        "--frame_num",
        type=int,
        default=None,
        help="How many frames to sample from a image or video. The number should be 4n+1",
    )
    parser.add_argument(
        "--ckpt_dir",
        type=str,
        # default="./Wan2.1-T2V-14B",
        default="./Wan2.1-T2V-1.3B",
        help="The path to the checkpoint directory.",
    )
    parser.add_argument(
        "--offload_model",
        type=str2bool,
        default=None,
        help="Whether to offload the model to CPU after each model forward, reducing GPU memory usage.",
    )
    parser.add_argument(
        "--ulysses_size",
        type=int,
        default=2,
        help="The size of the ulysses parallelism in DiT.",
    )
    parser.add_argument(
        "--ring_size",
        type=int,
        default=1,
        help="The size of the ring attention parallelism in DiT.",
    )
    parser.add_argument(
        "--t5_fsdp",
        action="store_true",
        # default=False,
        default=True,
        help="Whether to use FSDP for T5.",
    )
    parser.add_argument(
        "--t5_cpu",
        action="store_true",
        default=False,
        help="Whether to place T5 model on CPU.",
    )
    parser.add_argument(
        "--dit_fsdp",
        action="store_true",
        # default=False,
        default=True,
        help="Whether to use FSDP for DiT.",
    )
    parser.add_argument(
        "--save_file",
        type=str,
        default=None,
        help="The file to save the generated image or video to.",
    )
    parser.add_argument(
        "--src_video",
        type=str,
        default=None,
        help="The file of the source video. Default None.",
    )
    parser.add_argument(
        "--src_mask",
        type=str,
        default=None,
        help="The file of the source mask. Default None.",
    )
    parser.add_argument(
        "--src_ref_images",
        type=str,
        default=None,
        help="The file list of the source reference images. Separated by ','. Default None.",
    )
    parser.add_argument(
        "--prompt",
        type=str,
        default=None,
        help="The prompt to generate the image or video from.",
    )
    parser.add_argument(
        "--use_prompt_extend",
        action="store_true",
        default=False,
        help="Whether to use prompt extend.",
    )
    parser.add_argument(
        "--prompt_extend_method",
        type=str,
        default="local_qwen",
        choices=["dashscope", "local_qwen"],
        help="The prompt extend method to use.",
    )
    parser.add_argument(
        "--prompt_extend_model",
        type=str,
        default=None,
        help="The prompt extend model to use.",
    )
    parser.add_argument(
        "--prompt_extend_target_lang",
        type=str,
        default="zh",
        choices=["zh", "en"],
        help="The target language of prompt extend.",
    )
    parser.add_argument(
        "--base_seed",
        type=int,
        default=-1,
        help="The seed to use for generating the image or video.",
    )
    parser.add_argument(
        "--image",
        type=str,
        default=None,
        help="[image to video] The image to generate the video from.",
    )
    parser.add_argument(
        "--first_frame",
        type=str,
        default=None,
        help="[first-last frame to video] The image (first frame) to generate the video from.",
    )
    parser.add_argument(
        "--last_frame",
        type=str,
        default=None,
        help="[first-last frame to video] The image (last frame) to generate the video from.",
    )
    parser.add_argument(
        "--sample_solver",
        type=str,
        default="unipc",
        choices=["unipc", "dpm++"],
        help="The solver used to sample.",
    )
    parser.add_argument(
        "--sample_steps", type=int, default=None, help="The sampling steps."
    )
    parser.add_argument(
        "--sample_shift",
        type=float,
        default=None,
        help="Sampling shift factor for flow matching schedulers.",
    )
    parser.add_argument(
        "--sample_guide_scale",
        type=float,
        default=5.0,
        help="Classifier free guidance scale.",
    )

    args = parser.parse_args()

    _validate_args(args)

    return args


def _init_logging(rank):
    # logging
    if rank == 0:
        # set format
        logging.basicConfig(
            level=logging.INFO,
            format="[%(asctime)s] %(levelname)s: %(message)s",
            handlers=[logging.StreamHandler(stream=sys.stdout)],
        )
    else:
        logging.basicConfig(level=logging.ERROR)


def _get_rank():
    return dist.get_rank() if dist.is_initialized() else 0


class WAN21MayTask(MayTask):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Parse args
        args = _parse_args()
        self.args = args

        # Init
        rank = int(os.getenv("RANK", 0))
        world_size = int(os.getenv("WORLD_SIZE", 1))
        local_rank = int(os.getenv("LOCAL_RANK", 0))
        device = local_rank
        _init_logging(rank)

        ## Init distributed environment
        if args.offload_model is None:
            args.offload_model = False if world_size > 1 else True
            logging.info(
                f"offload_model is not specified, set to {args.offload_model}."
            )
        if world_size > 1:
            torch.cuda.set_device(local_rank)
            dist.init_process_group(
                backend="nccl", init_method="env://", rank=rank, world_size=world_size
            )
        else:
            assert not (args.t5_fsdp or args.dit_fsdp), (
                "t5_fsdp and dit_fsdp are not supported in non-distributed environments."
            )
            assert not (args.ulysses_size > 1 or args.ring_size > 1), (
                "context parallel are not supported in non-distributed environments."
            )

        if args.ulysses_size > 1 or args.ring_size > 1:
            assert args.ulysses_size * args.ring_size == world_size, (
                "The number of ulysses_size and ring_size should be equal to the world size."
            )
            from xfuser.core.distributed import (
                init_distributed_environment,
                initialize_model_parallel,
            )

            init_distributed_environment(
                rank=dist.get_rank(), world_size=dist.get_world_size()
            )

            initialize_model_parallel(
                sequence_parallel_degree=dist.get_world_size(),
                ring_degree=args.ring_size,
                ulysses_degree=args.ulysses_size,
            )

        if args.use_prompt_extend:
            if args.prompt_extend_method == "dashscope":
                self.prompt_expander = DashScopePromptExpander(
                    model_name=args.prompt_extend_model,
                    is_vl="i2v" in args.task or "flf2v" in args.task,
                )
            elif args.prompt_extend_method == "local_qwen":
                self.prompt_expander = QwenPromptExpander(
                    model_name=args.prompt_extend_model,
                    is_vl="i2v" in args.task,
                    device=rank,
                )
            else:
                raise NotImplementedError(
                    f"Unsupport prompt_extend_method: {args.prompt_extend_method}"
                )

        cfg = WAN_CONFIGS[args.task]
        if args.ulysses_size > 1:
            assert cfg.num_heads % args.ulysses_size == 0, (
                f"`{cfg.num_heads=}` cannot be divided evenly by `{args.ulysses_size=}`."
            )

        self.cfg = cfg
        logging.info(f"Generation model config: {cfg}")

        if dist.is_initialized():
            base_seed = [args.base_seed] if rank == 0 else [None]
            dist.broadcast_object_list(base_seed, src=0)
            args.base_seed = base_seed[0]

        logging.info("Creating WanT2V pipeline.")
        self.wan_t2v = wan.WanT2V(
            config=cfg,
            checkpoint_dir=args.ckpt_dir,
            device_id=device,
            rank=rank,
            t5_fsdp=args.t5_fsdp,
            dit_fsdp=args.dit_fsdp,
            use_usp=(args.ulysses_size > 1 or args.ring_size > 1),
            t5_cpu=args.t5_cpu,
        )

        # Put none 0 rank workers into a waiting state
        if _get_rank() != 0:
            while True:
                self._run({})

    def _run(self, payload: dict):
        rank = _get_rank()
        if dist.is_initialized():
            object_list = [payload]
            dist.broadcast_object_list(object_list, src=0)
            payload = object_list[0]

        args = self.args
        args.prompt = payload["text_prompt"]
        logging.info(f"Generation job args: {args}")

        logging.info(f"Input prompt: {args.prompt}")
        if args.use_prompt_extend:
            logging.info("Extending prompt ...")
            if rank == 0:
                prompt_output = self.prompt_expander(
                    args.prompt,
                    tar_lang=args.prompt_extend_target_lang,
                    seed=args.base_seed,
                )
                if not prompt_output.status:
                    logging.info(f"Extending prompt failed: {prompt_output.message}")
                    logging.info("Falling back to original prompt.")
                    input_prompt = args.prompt
                else:
                    input_prompt = prompt_output.prompt
                input_prompt = [input_prompt]
            else:
                input_prompt = [None]
            if dist.is_initialized():
                dist.broadcast_object_list(input_prompt, src=0)
            args.prompt = input_prompt[0]
            logging.info(f"Extended prompt: {args.prompt}")

        logging.info(f"Generating {'image' if 't2i' in args.task else 'video'} ...")
        video = self.wan_t2v.generate(
            args.prompt,
            size=SIZE_CONFIGS[args.size],
            frame_num=args.frame_num,
            shift=args.sample_shift,
            sample_solver=args.sample_solver,
            sampling_steps=args.sample_steps,
            guide_scale=args.sample_guide_scale,
            seed=args.base_seed,
            offload_model=args.offload_model,
        )

        if rank == 0:
            save_dir = os.path.join("outputs")
            os.makedirs(save_dir, exist_ok=True)
            save_name = f"{nanoid.generate()}.mp4"
            save_pathname = os.path.abspath(os.path.join(save_dir, save_name))
            logging.info(f"Saving generated video to {args.save_file}")
            cache_video(
                tensor=video[None],
                save_file=save_pathname,
                fps=self.cfg.sample_fps,
                nrow=1,
                normalize=True,
                value_range=(-1, 1),
            )
            self.save_result(
                dict(text_prompt=args.prompt, video_pathname=save_pathname)
            )

    def run(self, payload: dict):
        self._run(payload)


if __name__ == "__main__":
    local_rank = int(os.getenv("LOCAL_RANK", 0))
    torch.cuda.set_device(local_rank)
    task = WAN21MayTask(task_id="_LOPB-qlLCq49386Lb7z3")
    # task.run(dict(text_prompt="Generate a video of a cat playing with a ball."))
    if _get_rank() == 0:
        task.run_server()
