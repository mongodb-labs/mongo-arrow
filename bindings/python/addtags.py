# Dependencies:
# - auditwheel>=6,<7
# Requires AUDITWHEEL_PLAT to be set (e.g. manylinux2014_x86_64)
# Usage:
# $ python addtags.py WHEEL_PATH WHEEL_DIR
import os
from os.path import abspath, basename, exists, isfile
from os.path import join as pjoin
from sys import argv

from auditwheel.policy import WheelPolicies, get_replace_platforms
from auditwheel.wheel_abi import analyze_wheel_abi
from auditwheel.wheeltools import InWheelCtx, add_platforms


def repair_wheel(wheel_path, abi, wheel_dir):
    wheel_fname = basename(wheel_path)
    with InWheelCtx(wheel_path) as ctx:
        ctx.out_wheel = pjoin(wheel_dir, wheel_fname)
        ctx.out_wheel = add_platforms(ctx, [abi], get_replace_platforms(abi))
    return ctx.out_wheel


def main(wheel_path, abi, wheel_dir):
    if not isfile(wheel_path):
        msg = f"cannot access wheel file {wheel_path}"
        raise FileNotFoundError(msg)

    if not exists(wheel_dir):
        os.makedirs(wheel_dir)

    policies = WheelPolicies()
    reqd_tag = policies.get_priority_by_name(abi)
    out_wheel = repair_wheel(wheel_path, abi, wheel_dir)

    if out_wheel is not None:
        analyzed_tag = analyze_wheel_abi(policies, out_wheel, set()).overall_tag
        if reqd_tag < policies.get_priority_by_name(analyzed_tag):
            print(
                "Wheel is eligible for a higher priority tag. "
                f"You requested {abi} but I have found this wheel is "
                f"eligible for {analyzed_tag}."
            )
            out_wheel = repair_wheel(wheel_path, analyzed_tag, wheel_dir)

        print(f"Fixed-up wheel written to {out_wheel}")


if __name__ == "__main__":
    WHEEL_PATH, WHEEL_DIR = argv[1], argv[2]
    TARGET_PLATFORM = os.environ["AUDITWHEEL_PLAT"]
    print(f"wheel path: {WHEEL_PATH}")
    print(f"target platform: {TARGET_PLATFORM}")
    print(f"wheel dir: {WHEEL_DIR}")
    main(
        wheel_path=abspath(WHEEL_PATH),
        abi=TARGET_PLATFORM,
        wheel_dir=abspath(WHEEL_DIR),
    )
