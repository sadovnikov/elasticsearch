module Jekyll
  module Checked
    def checked(text)
      text.gsub(/\[x\]/, '<span class="ballot_box_with_check"></span>').gsub(/\[ \]/, '<span class="ballot_box"></span>')
      # %r{\[x\]}i
    end
  end
end

Liquid::Template.register_filter(Jekyll::Checked)