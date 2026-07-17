/*
 * Copyright 2022 WaterdogTEAM
 * Licensed under the GNU General Public License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.gnu.org/licenses/old-licenses/gpl-2.0.html
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.waterdog.waterdogpe.command.defaults;

import dev.waterdog.waterdogpe.WaterdogPE;
import dev.waterdog.waterdogpe.command.Command;
import dev.waterdog.waterdogpe.command.CommandSender;
import dev.waterdog.waterdogpe.command.CommandSettings;
import org.apache.logging.log4j.Level;

public class DebugCommand extends Command {

    public DebugCommand() {
        super("wddebug", CommandSettings.builder()
                .setDescription("waterdog.command.debug.description")
                .setUsageMessage("waterdog.command.debug.usage")
                .setPermission("waterdog.command.debug.permission")
                .build());
    }

    @Override
    public boolean onExecute(CommandSender sender, String alias, String[] args) {
        boolean enable;
        if (args.length > 0) {
            switch (args[0].toLowerCase()) {
                case "on", "true", "enable" -> enable = true;
                case "off", "false", "disable" -> enable = false;
                default -> {
                    sender.sendMessage("§cUsage: /" + alias + " [on|off]");
                    return true;
                }
            }
        } else {
            // No argument: flip the current state.
            enable = !WaterdogPE.version().debug();
        }

        // Flips the live log4j root level. version().debug(true) already switches it to DEBUG;
        // we only need to restore INFO ourselves when disabling.
        WaterdogPE.version().debug(enable);
        if (!enable) {
            WaterdogPE.setLoggerLevel(Level.INFO);
        }

        sender.sendMessage("§aDebug logging is now " + (enable ? "§2enabled" : "§cdisabled") + "§a.");
        return true;
    }
}
