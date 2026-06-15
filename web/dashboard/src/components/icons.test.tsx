import { expect, test } from "vitest";
import { render } from "@testing-library/react";
import {
  IconCron,
  IconLogo,
  IconOverview,
  IconQueues,
  IconTasks,
  IconWorkers,
  IconExternal,
  IconSun,
  IconMoon,
} from "./icons.tsx";

test("each icon renders an svg", () => {
  for (const Icon of [IconLogo, IconOverview, IconQueues, IconTasks, IconCron, IconWorkers, IconExternal, IconSun, IconMoon]) {
    const { container, unmount } = render(<Icon />);
    expect(container.querySelector("svg")).not.toBeNull();
    unmount();
  }
});
