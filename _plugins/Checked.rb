module Jekyll
  module Checked
    def checked(text)
      text.gsub(%r{\[x\]}i, '<span class="ballot_box_with_check"></span>').gsub(%r{\[ \]}i, '<span class="ballot_box"></span>')
    end
  end
end

Liquid::Template.register_filter(Jekyll::Checked)